from dataclasses import dataclass
import shutil
from epss import util, epss
from epss.constants import CACHE_DIR, DEFAULT_SORTING_KEY, DEFAULT_FILE_FORMAT, FILE_FORMATS, MIN_DATE, PARQUET, STRS, TIME
from typing import Optional, Iterable, Iterator, Set, Tuple
import concurrent.futures
import datetime
import functools 
import io
import logging
import os
import polars as pl
import re
import requests

logger = logging.getLogger(__name__)


@dataclass()
class Query:
    cve_ids: Optional[STRS] = None
    min_score: Optional[float] = None
    max_score: Optional[float] = None
    min_percentile: Optional[float] = None
    max_percentile: Optional[float] = None


def get_query(
        cve_ids: Optional[STRS] = None,
        min_score: Optional[float] = None,
        max_score: Optional[float] = None,
        min_percentile: Optional[float] = None,
        max_percentile: Optional[float] = None) -> Query:
    
    return Query(
        cve_ids=cve_ids,
        min_score=min_score,
        max_score=max_score,
        min_percentile=min_percentile,
        max_percentile=max_percentile,
    )


@dataclass()
class Client:
    """
    This client creates the following local directory structure:

    - `workdir` 
        - raw-scores-by-date
            - YYYY-MM-DD.parquet
        -changelogs-by-date
            - YYYY-MM-DD.parquet
        - changelogs-by-cve
            - CVE-YYYY-NNNN.parquet
    """
    workdir: str = CACHE_DIR
    file_format: str = PARQUET

    @property
    def raw_scores_by_date_dir(self) -> str:
        return os.path.join(self.workdir, 'raw-scores-by-date')

    @property
    def changelogs_by_date_dir(self) -> str:
        return os.path.join(self.workdir, 'changelogs-by-date')
    
    @property
    def changelogs_by_cve_dir(self) -> str:
        return os.path.join(self.workdir, 'changelogs-by-cve')

    @property
    def cve_ids(self) -> Set[str]:
        df = self.get_score_dataframe(date=self.max_date)
        return set(df['cve'].unique())

    @property
    def min_date(self) -> datetime.date:
        return get_min_date()
    
    @property
    def max_date(self) -> datetime.date:
        return get_max_date()
    
    def init(self, min_date: Optional[TIME] = None, max_date: Optional[TIME] = None):
        self.download_scores_over_time(min_date=min_date, max_date=max_date)
        self.create_partitions(min_date=min_date, max_date=max_date)

    def create_partitions(self, min_date: Optional[TIME] = None, max_date: Optional[TIME] = None):
        df = self.get_historical_diff_dataframe(min_date=min_date, max_date=max_date)
        partitions = {
            self.changelogs_by_date_dir: 'date',
            self.changelogs_by_cve_dir: 'cve',
        }
        for (output_dir, partitioning_key) in partitions.items():
            epss.write_partitioned_dataframe_to_dir(
                df=df,
                output_dir=output_dir,
                partitioning_key=partitioning_key,
                file_format=self.file_format,
                overwrite=True,
            )

    def clear(self, delete_downloads: bool = False):
        if os.path.exists(self.workdir):
            directories = {
                self.changelogs_by_cve_dir,
                self.changelogs_by_date_dir,
            }
            if delete_downloads:
                directories.add(self.raw_scores_by_date_dir)

            for directory in directories:
                if os.path.exists(directory):
                    logger.debug('Deleting %s', directory)
                    shutil.rmtree(directory, ignore_errors=True)

    def download_scores_over_time(
            self, 
            min_date: Optional[TIME] = None, 
            max_date: Optional[TIME] = None,
            overwrite: bool = False):

        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {}
            for date in util.iter_dates_in_range(min_date=min_date, max_date=max_date):
                path = get_file_path(directory=self.raw_scores_by_date_dir, date=date, file_format=self.file_format)
                if os.path.exists(path) and not overwrite:
                    continue

                future = executor.submit(
                    download_scores_by_date, 
                    date=date, 
                    path=path,
                )
                futures[future] = date
            
            if futures:
                futures = concurrent.futures.as_completed(futures)
                for future in futures:
                    future.result()

    def download_scores_by_date(
            self,
            date: TIME,
            path: str,
            cve_ids: Optional[STRS] = None):
        
        return download_scores_by_date(
            date=date,
            path=path,
            cve_ids=cve_ids,
        )

    def get_score_dataframe(
            self,
            date: Optional[TIME] = None,
            query: Optional[Query] = None) -> pl.DataFrame:

        date = util.parse_date(date or self.max_date)
        path = get_file_path(directory=self.raw_scores_by_date_dir, date=date, file_format=self.file_format)
        if not os.path.exists(path):
            self.download_scores_by_date(date=date)

        df = self.read_dataframe(path, query=query)
        return df
    
    def read_dataframe(
            self,
            path: str,
            query: Optional[Query] = None) -> pl.DataFrame:

        return read_dataframe(path=path, query=query)
        
    def get_historical_diff_dataframe(
            self,
            query: Optional[Query] = None,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None,
            preserve_order: bool = True) -> pl.DataFrame:

        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)
    
        logger.debug('Generating changelog dataframe for %s - %s', min_date, max_date)
        start_time = datetime.datetime.now()

        dfs = self.iter_score_diff_dataframes(
            query=query,
            min_date=min_date, 
            max_date=max_date, 
            preserve_order=False,
        )
        df = pl.concat(dfs)
        if preserve_order:
            df = df.sort(by=DEFAULT_SORTING_KEY)

        end_time = datetime.datetime.now()
        logger.debug('Generated changelog dataframe in %.2f', (end_time - start_time).total_seconds())
        logger.debug('Shape of changelogs dataframe: %s', df.shape)
        return df
    
    def get_score_diff_dataframe(
            self, 
            first_date: TIME, 
            second_date: TIME, 
            query: Optional[Query] = None) -> pl.DataFrame:
        
        first_date = util.parse_date(first_date)
        second_date = util.parse_date(second_date)

        path = get_file_path(directory=self.changelogs_by_date_dir, date=second_date, file_format=self.file_format)
        if os.path.exists(path):
            df = self.read_dataframe(path=path, query=query)
        else:
            a = self.get_score_dataframe(date=first_date, query=query)
            b = self.get_score_dataframe(date=second_date, query=query)
            df = get_diff(a, b)
            util.write_dataframe(df=df, path=path)
        
        return df

    def iter_score_diff_dataframes(
            self, 
            query: Optional[Query] = None, 
            min_date: Optional[TIME] = None, 
            max_date: Optional[TIME] = None,
            preserve_order: bool = True) -> Iterator[pl.DataFrame]:
        
        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)
        dates = util.iter_dates_in_range(min_date=min_date, max_date=max_date)

        if preserve_order:
            for (a, b) in util.iter_pairwise(dates):
                df = self.get_score_diff_dataframe(first_date=a, second_date=b, query=query)
                yield df
        else:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for (a, b) in util.iter_pairwise(dates):
                    future = executor.submit(self.get_score_diff_dataframe, a, b, query=query)
                    futures.append(future)
                
                futures = concurrent.futures.as_completed(futures)
                for future in futures:
                    df = future.result()
                    yield df

    def get_score_by_cve_id(self, cve_id: str, date: Optional[TIME] = None) -> Optional[float]:
        query = get_query(cve_ids=[cve_id])
        df = self.get_score_dataframe(query=query, date=date)
        return df['epss'].max()
    
    def get_score_range_dataframe(
            self,
            query: Optional[Query] = None,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None,
            preserve_order: bool = True) -> pl.DataFrame:
        
        df = self.get_historical_diff_dataframe(
            query=query,
            min_date=min_date,
            max_date=max_date,
            preserve_order=False,
        )
        df = df.groupby('cve').agg([
            pl.col('epss').min().alias('min_epss'),
            pl.col('epss').max().alias('max_epss'),
            pl.col('percentile').min().alias('min_percentile'),
            pl.col('percentile').max().alias('max_percentile'),
        ])
        df = df.with_columns(
            epss_change=pl.col('max_epss') - pl.col('min_epss'),
            percentile_change=pl.col('max_percentile') - pl.col('min_percentile'),
        )
        df = df.with_columns(
            epss_change_pct=(pl.col('epss_change') / pl.col('min_epss')) * 100,
            percentile_change_pct=(pl.col('percentile_change') / pl.col('min_percentile')) * 100,
        )
        df = util.rejig_dataframe_precision(df=df, n=5)
        if preserve_order:
            df = df.sort(by=DEFAULT_SORTING_KEY)
        return df
    
    def get_score_range_tuple_by_cve_id(
            self,
            cve_id: str,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None) -> Tuple[float, float]:
        
        query = get_query(cve_ids=[cve_id])
        df = self.get_score_range_dataframe(query=query, min_date=min_date, max_date=max_date)
        if len(df) == 0:
            raise ValueError(f"No EPSS scores found for {cve_id}")
        
        o = df.row(0, named=True)
        return (o['min_epss'], o['max_epss'])
    
    def get_percentile_range_tuple_by_cve_id(
            self,
            cve_id: str,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None) -> Tuple[float, float]:
        
        query = get_query(cve_ids=[cve_id])
        df = self.get_score_range_dataframe(query=query, min_date=min_date, max_date=max_date)
        if len(df) == 0:
            raise ValueError(f"No EPSS scores found for {cve_id}")
        
        o = df.row(0, named=True)
        return (o['min_percentile'], o['max_percentile'])


def read_dataframe(path: str, query: Optional[Query] = None) -> pl.DataFrame:
    df = util.read_dataframe(path)

    # Insert the `date` column if it's missing.
    if 'date' not in df.columns:
        date = util.get_date_from_filename(path)
        df = df.with_columns(
            date=date,
        )

    if query:
        df = filter_dataframe_with_query(df=df, query=query)
    return df


def read_dataframes(
        paths: STRS,
        query: Optional[Query] = None,
        preserve_order: bool = False) -> Iterator[pl.DataFrame]:
    
    f = functools.partial(read_dataframe, query=query)
    if preserve_order:
        yield from map(f, paths)
    else:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            yield from executor.map(f, paths)


def filter_dataframes_with_query(dfs: pl.DataFrame, query: Optional[Query]) -> Iterator[pl.DataFrame]:
        for df in dfs:
            if query:
                df = filter_dataframe_with_query(df=df, query=query)
            yield df


def filter_dataframe_with_query(df: pl.DataFrame, query: Optional[Query] = None) -> pl.DataFrame:
    if query:
        df = filter_dataframe(
            df=df,
            cve_ids=query.cve_ids,
            min_score=query.min_score,
            max_score=query.max_score,
            min_percentile=query.min_percentile,
            max_percentile=query.max_percentile,
        )
    return df


def filter_dataframes(
        dfs: Iterable[pl.DataFrame],
        cve_ids: Optional[STRS] = None,
        min_score: Optional[float] = None,
        max_score: Optional[float] = None,
        min_percentile: Optional[float] = None,
        max_percentile: Optional[float] = None,
        min_date: Optional[TIME] = MIN_DATE,
        max_date: Optional[TIME] = None) -> Iterator[pl.DataFrame]:
    
    f = functools.partial(
        filter_dataframe,
        cve_ids=cve_ids,
        min_score=min_score,
        max_score=max_score,
        min_percentile=min_percentile,
        max_percentile=max_percentile,
        min_date=min_date,
        max_date=max_date,
    )
    yield from map(f, dfs)


def filter_dataframe(
        df: pl.DataFrame,
        cve_ids: Optional[STRS] = None,
        min_score: Optional[float] = None,
        max_score: Optional[float] = None,
        min_percentile: Optional[float] = None,
        max_percentile: Optional[float] = None,
        min_date: Optional[TIME] = None,
        max_date: Optional[TIME] = None) -> pl.DataFrame:

        min_date = util.parse_date(min_date) if min_date else None
        max_date = util.parse_date(max_date) if max_date else None

        predicates = []

        if cve_ids:
            predicates.append(pl.col('cve').is_in(cve_ids))

        if min_date:
            predicates.append(pl.col('date') >= min_date)
        
        if max_date:
            predicates.append(pl.col('date') <= max_date)

        if min_score:
            predicates.append(pl.col('epss') >= min_score)
        
        if max_score:
            predicates.append(pl.col('epss') <= max_score)

        if min_percentile:
            predicates.append(pl.col('percentile') >= min_percentile)
        
        if max_percentile:
            predicates.append(pl.col('percentile') <= max_percentile)
        
        if min_percentile:
            predicates.append(pl.col('percentile') >= min_percentile)

        if max_percentile:
            predicates.append(pl.col('percentile') <= max_percentile)

        if predicates:
            df = df.filter(predicates)
        
        return df


def write_partitioned_dataframe_to_dir(
        df: pl.DataFrame,
        output_dir: str,
        partitioning_key: str,
        file_format: str = DEFAULT_FILE_FORMAT,
        overwrite: bool = False):
    """
    Partition the provided dataframe into (p) partitions where (p) is the number of unique values for the given partitioning key.

    Performance metrics on a dataframe with ~550,000 rows and 11 columns on a 2021 MacBook Pro:

    - 550 partitions: 2 seconds
    - ~220,000 partitions: 
    """
    def get_path(_k: str) -> str:
        return os.path.join(output_dir, f"{_k}.{file_format}")
    
    file_format = file_format or DEFAULT_FILE_FORMAT
    total_partitions = len(df[partitioning_key].unique())
    logger.debug('Partitioning dataframe with %d rows into %d partitions', len(df), total_partitions)

    # Identify existing partitions.
    existing_paths = None
    if overwrite is False:
        existing_paths = frozenset(util.iter_paths(output_dir))
    
    # Partition the dataframe.
    start_time = datetime.datetime.now()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {}
        for (k, p) in partition_dataframe(df=df, partitioning_key=partitioning_key):
            path = get_path(k)
            if overwrite is False and path in existing_paths:
                continue

            future = executor.submit(util.write_dataframe, df=p, path=path)
            futures[future] = path  
    
        if futures:
            total_rows, total_columns = df.shape
            total_partitions = len(futures)

            logger.debug('Creating %d partitions over dataframe with %d rows and %d columns', total_partitions, total_rows, total_columns)
            futures = concurrent.futures.as_completed(futures)
            for future in futures:
                future.result()
    
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.debug('Created %d partitions over dataframe with %d rows and %d columns in %.2f seconds', total_partitions, total_rows, total_columns, duration)


def partition_dataframe(
        df: pl.DataFrame,
        partitioning_key: str = DEFAULT_SORTING_KEY) -> Iterator[Tuple[str, pl.DataFrame]]:
    
    if partitioning_key == 'cve':
        yield from partition_dataframe_by_cve_id(df=df)
    elif partitioning_key == 'date':
        yield from partition_dataframe_by_date(df=df)
    else:
        raise ValueError(f"Unsupported partitioning key: {partitioning_key}")


def partition_dataframe_by_cve_id(df: pl.DataFrame) -> Iterator[Tuple[str, pl.DataFrame]]:
    for cve_id in df['cve'].unique():
        yield (cve_id, df.filter(pl.col('cve') == cve_id))


def partition_dataframe_by_date(df: pl.DataFrame) -> Iterator[Tuple[str, pl.DataFrame]]:
    for date in df['date'].unique():
        yield (date, df.filter(pl.col('date') == date))


def download_scores_over_time(
        output_dir: str,
        file_format: str = DEFAULT_FILE_FORMAT,
        cve_ids: Optional[STRS] = None,
        min_date: Optional[TIME] = None,
        max_date: Optional[TIME] = None):

    min_date = util.parse_date(min_date or get_min_date())
    max_date = util.parse_date(max_date or get_max_date())

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for date in util.iter_dates_in_range(min_date=min_date, max_date=max_date):
            path = get_file_path(directory=output_dir, date=date, file_format=file_format)
            if os.path.exists(path):
                continue

            future = executor.submit(
                download_scores_by_date, 
                date=date, 
                path=path,
                cve_ids=cve_ids,                 
            )
            futures.append(future)
        
        if futures:
            futures = concurrent.futures.as_completed(futures)
            for future in futures:
                future.result()
        

def download_scores_by_date(
        date: TIME,
        path: str,
        query: Optional[Query] = None):
    
    date = util.parse_date(date)
    url = get_download_url(date=date)

    logger.debug('Downloading scores for %s from %s to %s', date, url, path)
    response = requests.get(url, stream=True)
    response.raise_for_status()

    data = io.BytesIO(response.content)
    df = pl.read_csv(data, skip_rows=1)

    df.with_columns(
        date=date,
    )
    if query:
        df = filter_dataframe_with_query(df=df, query=query)
        if df.is_empty():
            logger.warning('No matching scores found for %s', date)
            return

    util.write_dataframe(df=df, path=path)
    logger.debug('Downloaded scores for %s', date)


def get_file_path(
        date: TIME,
        directory: str, 
        file_format: str) -> str:
    
    assert date is not None, "No date provided"
    date = util.parse_date(date)

    assert file_format in FILE_FORMATS, f"Unsupported file format: {file_format}"
    return os.path.join(directory, f"{date.isoformat()}.{file_format}")


def get_download_url(date: Optional[TIME] = None) -> str:
    date = util.parse_date(date) if date else get_max_date()
    return f"https://epss.cyentia.com/epss_scores-{date.isoformat()}.csv.gz"


def get_min_date() -> datetime.date:
    return util.parse_date(MIN_DATE)


def get_max_date() -> datetime.date:
    url = "https://epss.cyentia.com/epss_scores-current.csv.gz"
    logger.debug('Resolving latest publication date for EPSS scores via %s', url)

    response = requests.head(url)
    location = response.headers["Location"]
    assert location is not None, "No Location header found"
    regex = r"(\d{4}-\d{2}-\d{2})"
    match = re.search(regex, location)
    assert match is not None, f"No date found in {location}"
    date = datetime.date.fromisoformat(match.group(1))

    logger.debug('Latest publication date for EPSS scores is %s', date)
    return date


def get_diff(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
    """
    Adds the following columns:
    - old_epss
    - epss_change
    - epss_change_pct
    - old_percentile
    - percentile_change
    - percentile_change_pct
    - old_date
    """
    df = pl.concat([a, b])
    df = df.sort(by=['date', 'cve'])
    total_before = len(a)
    
    df = df.with_columns(
        old_date=pl.col('date').shift().over('cve'),
        old_epss=pl.col('epss').shift().over('cve'),
        old_percentile=pl.col('percentile').shift().over('cve'),
    )
    df = df.with_columns(
        epss_change=pl.col('epss') - pl.col('old_epss'),
        percentile_change=pl.col('percentile') - pl.col('old_percentile'),
    )
    df = df.with_columns(
        epss_change_pct=(pl.col('epss_change') / pl.col('old_epss')) * 100,
        percentile_change_pct=(pl.col('percentile_change') / pl.col('old_percentile')) * 100,
    )
    df = util.rejig_dataframe_precision(df=df, n=5)

    # Drop rows where old_epss is not null and epss_change is 0.
    df = df.filter(pl.col('old_epss').is_not_null() & (pl.col('epss_change') != 0))

    # Reorder columns.
    df = df.select([
        'cve',
        'date',
        'epss',
        'percentile',
        'old_date',
        'old_epss',
        'old_percentile',
        'epss_change',
        'epss_change_pct',
        'percentile_change',
        'percentile_change_pct',
    ])
    total_after = len(df)
    logger.debug('Dropped %d rows with no change (%.2f%%)', total_before - total_after, ((total_before - total_after) / total_before) * 100)
    return df

