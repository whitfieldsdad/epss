from dataclasses import dataclass
import datetime
import functools
import io
import itertools
import os
import re
from typing import Any, Iterable, Iterator, Optional, Tuple, Union

import requests
from epss import util
from epss.constants import DEFAULT_FILE_FORMAT, TIME, V1_RELEASE_DATE, V2_RELEASE_DATE, V3_RELEASE_DATE
import polars as pl
import concurrent.futures

import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Query:
    cve_ids: Optional[Iterable[str]] = None
    min_epss: Optional[float] = None
    max_epss: Optional[float] = None
    min_percentile: Optional[float] = None
    max_percentile: Optional[float] = None


@dataclass()
class ClientInterface:
    def get_scores(
            self, 
            workdir: str,
            min_date: Optional[TIME] = None, 
            max_date: Optional[TIME] = None,
            query: Optional[Query] = None,
            drop_unchanged_scores: bool = True) -> Any:
        """
        Returns a dataframe containing EPSS scores published between the specified dates.
        
        The dataframe will be sorted by date and CVE ID in descending order.
        """
        raise NotImplementedError()
    
    def get_scores_by_date(
            self,
            workdir: str, 
            date: Optional[TIME] = None,
            query: Optional[Query] = None) -> Any:
        """
        Returns a dataframe containing EPSS scores published on the specified date.

        The dataframe will be sorted by CVE ID in descending order.
        """
        raise NotImplementedError()


@dataclass()
class BaseClient(ClientInterface):
    file_format: str = DEFAULT_FILE_FORMAT
    verify_tls: bool = True
    include_v1_scores: bool = False
    include_v2_scores: bool = False
    include_v3_scores: bool = True

    @property
    def min_date(self) -> datetime.date:
        return self.get_min_date()
    
    @property
    def max_date(self) -> datetime.date:
        return self.get_max_date()
    
    @property
    def date_range(self) -> Tuple[datetime.date, datetime.date]:
        return self.get_date_range()

    def get_min_date(self) -> datetime.date:
        """
        Returns the earliest publication date for EPSS scores under the specified model version constraints.
        """
        return get_min_date(
            include_v1_scores=self.include_v1_scores,
            include_v2_scores=self.include_v2_scores,
            include_v3_scores=self.include_v3_scores,
        )

    def get_max_date(self) -> datetime.date:
        """
        Returns the latest publication date for EPSS scores under the specified model version constraints.
        """
        return get_max_date(
            include_v1_scores=self.include_v1_scores,
            include_v2_scores=self.include_v2_scores,
            include_v3_scores=self.include_v3_scores,
            verify_tls=self.verify_tls,
        )
    
    def get_date_range(self, min_date: Optional[TIME] = None, max_date: Optional[TIME] = None) -> Tuple[datetime.date, datetime.date]:
        """
        Returns a tuple containing the earliest and latest publication dates for EPSS scores under the specified model version constraints.
        """
        min_allowed_date = self.get_min_date()
        max_allowed_date = self.get_max_date()
        logger.debug('Detected allowed date range as: %s - %s', min_allowed_date, max_allowed_date)
        
        min_date = util.parse_date(min_date) if min_date else min_allowed_date
        max_date = util.parse_date(max_date) if max_date else max_allowed_date

        if min_date < min_allowed_date:
            min_date = min_allowed_date

        if max_date > max_allowed_date:
            max_date = max_allowed_date

        return min_date, max_date
    
    def iter_dates(self, min_date: Optional[TIME] = None, max_date: Optional[TIME] = None) -> Iterator[datetime.date]:
        """
        Returns an iterator that yields dates in the range [min_date, max_date].
        """
        min_date, max_date = self.get_date_range(min_date=min_date, max_date=max_date)
        yield from util.iter_dates_in_range(min_date, max_date)
    
    def download_scores(
            self,
            workdir: str,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None):
        """
        Download EPSS scores published between the specified dates.
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {}
            for date in self.iter_dates(min_date, max_date):
                path = get_file_path(
                    workdir=workdir,
                    file_format=self.file_format,
                    key=date.isoformat(),
                )
                if not os.path.exists(path):
                    future = executor.submit(
                        self.download_scores_by_date,
                        workdir=workdir,
                        date=date,
                    )
                    futures[future] = date
            
            if futures:
                total = len(futures)
                min_date = min(futures.values())
                max_date = max(futures.values())
                logger.debug('Downloading scores for %s - %s (%d dates)', min_date.isoformat(), max_date.isoformat(), total)
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except requests.exceptions.HTTPError as e:
                        logger.warning('Failed to download scores for %s: %s', futures[future].isoformat(), e)
         
            logger.debug("All scores have been downloaded")

    def download_scores_by_date(self, workdir: str, date: TIME):
        """
        Download EPSS scores published on the specified date.
        """
        date = util.parse_date(date)
        path = get_file_path(
            workdir=workdir, 
            file_format=self.file_format, 
            key=date.isoformat(),
        )
        if os.path.exists(path):
            logger.debug("Scores for %s have already been downloaded: %s", date.isoformat(), path)
            return
        
        url = get_download_url(date, verify_tls=self.verify_tls)
        logger.debug('Downloading scores for %s: %s -> %s', date.isoformat(), url, path)

        response = requests.get(url, verify=self.verify_tls, stream=True)
        response.raise_for_status()

        data = io.BytesIO(response.content)

        if date <= util.parse_date('2022-01-01'):
            skip_rows = 0
        else:
            skip_rows = 1

        df = pl.read_csv(data, skip_rows=skip_rows)
        df.with_columns(
            date=date,
        )
        util.write_dataframe(df=df, path=path)


@dataclass()
class PolarsClient(BaseClient):
    """
    A client for working with EPSS scores using Polars DataFrames.
    """
    def get_scores(
            self, 
            workdir: str,
            min_date: Optional[TIME] = None, 
            max_date: Optional[TIME] = None,
            query: Optional[Query] = None,
            drop_unchanged_scores: bool = True) -> pl.DataFrame:
        
        min_date, max_date = self.get_date_range(min_date, max_date)
        
        if min_date == max_date:
            return self.get_scores_by_date(workdir=workdir, date=min_date, query=query)
        
        resolver = functools.partial(
            self.get_scores_by_date,
            workdir=workdir,
            query=query,
        )
        dates = self.iter_dates(min_date, max_date)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            dfs = executor.map(lambda date: resolver(date=date), dates)
            if drop_unchanged_scores is False:
                df = pl.concat(dfs)
            else:            
                first = next(dfs)
                changes = executor.map(lambda e: get_changed_scores(*e), util.iter_pairwise(dfs))

                df = pl.concat(itertools.chain([first], changes))
            
            df = df.sort(by=['date', 'cve'], descending=False)
            return df

    def get_scores_by_date(
            self,
            workdir: str, 
            date: Optional[TIME] = None,
            query: Optional[Query] = None) -> pl.DataFrame:
        
        date = util.parse_date(date)
        path = get_file_path(
            workdir=workdir,
            file_format=self.file_format,
            key=date.isoformat(),
        )
        if not os.path.exists(path):
            self.download_scores_by_date(workdir=workdir, date=date)
            assert os.path.exists(path), "Scores unexpectedly not downloaded"

        df = read_dataframe(path)
        if query:
            df = self.filter_scores(df, query)

        # Check if the dataframe contains a `cve` column
        if 'cve' not in df.columns:
            raise ValueError(f'The dataframe for {date.isoformat()} does not contain a `cve` column (columns: {df.columns})')

        df = df.sort(by=['cve'], descending=False)
        return df
    
    def filter_scores(self, df: pl.DataFrame, query: Query) -> pl.DataFrame:
        min_date, max_date = self.get_date_range()
        df = df.filter(pl.col('date') >= min_date)
        df = df.filter(pl.col('date') <= max_date)

        if query.cve_ids:
            df = df.filter(pl.col('cve').str.contains('|'.join(query.cve_ids)))

        if query.min_epss:
            df = df.filter(pl.col('epss') >= query.min_epss)
        
        if query.max_epss:
            df = df.filter(pl.col('epss') <= query.max_epss)
        
        if query.min_percentile:
            df = df.filter(pl.col('percentile') >= query.min_percentile)
        
        if query.max_percentile:
            df = df.filter(pl.col('percentile') <= query.max_percentile)
        
        return df
    
    def iter_urls(
            self,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None) -> Iterator[str]:
        
        min_date, max_date = self.get_date_range(min_date, max_date)
        for date in self.iter_dates(min_date, max_date):
            yield get_download_url(date, verify_tls=self.verify_tls)


def get_file_path(workdir: str, file_format: str, key: Union[datetime.date, str]) -> str:
    """
    File paths are constructed using the following pattern: {workdir}/{key}.{file_format}

    For example, if `workdir` is `/tmp/epss`, and `file_format` is `parquet`:

    - If partitioning by `date`: `/tmp/epss/2024-01-01.parquet`
    - If partitioning by `cve`: `/tmp/epss/CVE-2024-01-01.parquet`
    """
    if isinstance(key, datetime.date):
        key = key.isoformat()
    return os.path.join(workdir, f'{key}.{file_format}')


def get_download_url(date: Optional[TIME] = None, verify_tls: bool = True) -> str:
    """
    Returns the URL for downloading EPSS scores for the specified date.
    
    If no date is provided, the URL for the latest EPSS scores is returned.

    The date can be provided as a string in ISO-8601 format (YYYY-MM-DD), a datetime.date, datetime.datetime, or a UNIX timestamp.
   
    Example download URL: 

    - https://epss.cyentia.com/epss_scores-2024-01-01.csv.gz
    """
    date = util.parse_date(date) if date else get_max_date(verify_tls=verify_tls)
    return f"https://epss.cyentia.com/epss_scores-{date.isoformat()}.csv.gz"


def get_min_date(
        include_v1_scores: bool = False, 
        include_v2_scores: bool = False,
        include_v3_scores: bool = True) -> datetime.date:
    """
    Returns the earliest publication date for EPSS scores under the specified model version constraints.
    """
    if include_v1_scores:
        return get_epss_v1_min_date()
    elif include_v2_scores:
        return get_epss_v2_min_date()
    elif include_v3_scores:
        return get_epss_v3_min_date()
    else:
        logger.warning('Cannot exclude all versions of EPSS scores. Defaulting to EPSS v3.')
        return get_epss_v3_min_date()


def get_epss_v1_min_date() -> datetime.date:
    """
    Returns the earliest publication date for EPSS v1 scores.
    """
    return util.parse_date(V1_RELEASE_DATE)


def get_epss_v1_max_date() -> datetime.date:
    """
    Returns the latest publication date for EPSS v1 scores.
    """
    return get_epss_v2_min_date() - datetime.timedelta(days=1)


def get_epss_v2_min_date() -> datetime.date:
    """
    Returns the earliest publication date for EPSS v2 scores.
    """
    return util.parse_date(V2_RELEASE_DATE)


def get_epss_v2_max_date() -> datetime.date:
    """
    Returns the latest publication date for EPSS v2 scores.
    """
    return get_epss_v3_min_date() - datetime.timedelta(days=1)


def get_epss_v3_min_date() -> datetime.date:
    """
    Returns the earliest publication date for EPSS v3 scores.
    """
    return util.parse_date(V3_RELEASE_DATE)


def get_epss_v3_max_date(verify_tls: bool = True) -> datetime.date:
    """
    Returns the latest publication date for EPSS v3 scores.
    """
    url = "https://epss.cyentia.com/epss_scores-current.csv.gz"
    logger.debug("Resolving latest publication date for EPSS scores")

    response = requests.head(url, verify=verify_tls)
    location = response.headers["Location"]
    assert location is not None, "No Location header found"
    regex = r"(\d{4}-\d{2}-\d{2})"
    match = re.search(regex, location)
    assert match is not None, f"No date found in {location}"
    date = datetime.date.fromisoformat(match.group(1))

    logger.debug(f'EPSS scores were last published on {date.isoformat()}')
    return date


def get_max_date(
        include_v1_scores: bool = False,
        include_v2_scores: bool = False,
        include_v3_scores: bool = True,
        verify_tls: bool = True) -> datetime.date:
    """
    Returns the latest publication date for EPSS scores under the specified model version constraints.
    """
    if include_v3_scores:
        return get_epss_v3_max_date(verify_tls=verify_tls)
    elif include_v2_scores:
        return get_epss_v2_max_date()
    elif include_v1_scores:
        return get_epss_v1_max_date()
    else:
        logger.warning('Cannot exclude all versions of EPSS scores. Defaulting to EPSS v3.')
        return get_epss_v3_max_date(verify_tls=verify_tls)


def get_date_range(
        include_v1_scores: bool = False,
        include_v2_scores: bool = False,
        include_v3_scores: bool = True,
        verify_tls: bool = True) -> Tuple[datetime.date, datetime.date]:
    """
    Resolve the earliest and latest publication dates for EPSS scores under the specified model version constraints.
    """
    min_date = get_min_date(
        include_v1_scores=include_v1_scores,
        include_v2_scores=include_v2_scores,
        include_v3_scores=include_v3_scores,
    )
    max_date = get_max_date(
        include_v1_scores=include_v1_scores,
        include_v2_scores=include_v2_scores,
        include_v3_scores=include_v3_scores,
        verify_tls=verify_tls,
    )
    return min_date, max_date


def get_date_range(verify_tls: bool = True) -> Tuple[datetime.date, datetime.date]:
    """
    Returns a tuple containing the earliest and latest publication dates for EPSS scores.
    """
    return get_min_date(), get_max_date(verify_tls=verify_tls)


def get_changed_scores(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
    """
    Given two dataframes, `a` and `b`, this function returns a new dataframe containing only the rows where the `epss` column has changed.
    
    The dataframes are expected to have the following columns:
    - `date`: a date in ISO-8601 format
    - `cve`: a CVE ID (e.g. CVE-2021-1234)
    - `epss`: a floating point number representing the EPSS score for the CVE (e.g. 0.1234)
    """
    df = pl.concat([a, b])
    df = df.sort(by=['date', 'cve'])
    df = df.with_columns(
        prev_epss=pl.col('epss').shift().over('cve'),
    )
    df = df.with_columns(
        epss_change=pl.col('epss') - pl.col('prev_epss'),
    )
    df = df.filter(pl.col('epss_change') != 0)
    df = df.drop('prev_epss', 'epss_change')
    return df


def read_dataframe(path: str, date: Optional[TIME] = None) -> pl.DataFrame:
    """
    To support transformations over time, it's important to include a `date` column in the dataframe.

    If the `date` column is missing and not explicitly provided, it must be possible to infer it from the filename. In such cases, the filename must contain a date in ISO-8601 format (YYYY-MM-DD) (e.g. epss_scores-2024-01-01.csv.gz).
    """
    df = util.read_dataframe(path)
    logger.debug('Read dataframe from %s (shape: %s, columns: %s)', path, df.shape, df.columns)

    if 'date' not in df.columns:
        if date:
            date = util.parse_date(date)
        else:
            date = util.get_date_from_filename(path)
            assert date is not None, "ISO-8601 date not found in filename (YYYY-MM-DD)"

        df = df.with_columns(date=date)

    return df
