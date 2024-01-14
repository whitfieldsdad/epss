from dataclasses import dataclass
import shutil
import sys
from epss import util
from epss.constants import CACHE_DIR, DATE_AND_CVE, DEFAULT_FILE_FORMAT, FILE_FORMATS, MIN_DATE, OVERWRITE, PARQUET, STRS, TIME
from typing import Any, Dict, List, Optional, Iterable, Iterator, Set, Tuple
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


@dataclass
class Score:
    cve: str
    epss: float
    percentile: float
    date: datetime.date

    @property
    def cve_id(self) -> str:
        return self.cve
    
    @property
    def score(self) -> float:
        return self.epss


@dataclass
class ScoreChangelogEntry:
    date: datetime.date
    cve: str
    new_epss: float
    old_epss: Optional[float]
    epss_change: Optional[float]
    epss_change_pct: Optional[float]
    new_percentile: float
    old_percentile: Optional[float]
    percentile_change: Optional[float] 
    percentile_change_pct: Optional[float]

    @property
    def cve_id(self) -> str:
        return self.cve

    @property
    def score(self) -> Score:
        return self.new_epss

    def to_score(self) -> Score:
        return Score(
            cve=self.cve,
            epss=self.new_epss,
            percentile=self.new_percentile,
            date=self.date,
        )


@dataclass
class ScoreHistory:
    scores: Dict[datetime.date, Score]


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
        - raw-scores-by-date
            - YYYY-MM-DD.parquet
        -score-changelogs-by-date
            - YYYY-MM-DD.parquet
        - score-changelogs-by-cve-id
            - CVE-YYYY-NNNNN.parquet
        - facts
            - score-ranges.parquet
            - date-ranges.parquet
    """
    workdir: str = CACHE_DIR
    file_format: str = PARQUET

    @property
    def raw_scores_by_date_dir(self) -> str:
        return os.path.join(self.workdir, 'raw-scores-by-date')

    @property
    def score_changelogs_by_date_dir(self) -> str:
        return os.path.join(self.workdir, 'score-changelogs-by-date')

    def clear(self):
        self.clear_raw_scores_by_date_dir()
        self.clear_score_changelogs_by_date_dir()

    def clear_raw_scores_by_date_dir(self):
        shutil.rmtree(self.raw_scores_by_date_dir, ignore_errors=True)

    def clear_score_changelogs_by_date_dir(self):
        shutil.rmtree(self.score_changelogs_by_date_dir, ignore_errors=True)

    @property
    def cve_ids(self) -> Set[str]:
        df = self.get_scores_dataframe_by_date(date=self.max_date)
        return set(df['cve'].unique())

    @property
    def min_date(self) -> datetime.date:
        return get_min_date()
    
    @property
    def max_date(self) -> datetime.date:
        return get_max_date()
    
    def init(self, min_date: Optional[TIME] = None, max_date: Optional[TIME] = None):
        """
        Build the local cache of EPSS scores.
        """
        self.download_scores_over_time(min_date=min_date, max_date=max_date)
        self.build_score_changelogs()
    
    def build_score_changelogs(self, min_date: Optional[TIME] = None, max_date: Optional[TIME] = None):
        for df in self.iter_score_changelog_dataframes(min_date=min_date, max_date=max_date):
            date = df['date'].iloc[0]
            path = get_file_path(
                directory=self.score_changelogs_by_date_dir, 
                date=date, 
                file_format=self.file_format,
            )
            util.write_polars_dataframe(df=df, path=path)

    def download_scores_over_time(
            self, 
            min_date: Optional[TIME] = None, 
            max_date: Optional[TIME] = None,
            overwrite: bool = False):
        """
        Download daily sets of EPSS scores over the given date range.
        """
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

    def get_scores_dataframe_by_date(
            self,
            date: Optional[TIME] = None,
            query: Optional[Query] = None) -> pl.DataFrame:
        
        date = util.parse_date(date or self.max_date)
        path = get_file_path(directory=self.raw_scores_by_date_dir, date=date, file_format=self.file_format)
        if not os.path.exists(path):
            self.download_scores_by_date(date=date)

        df = read_dataframe(path)
        if query:
            df = filter_dataframe_with_query(df=df, query=query)
        return df
    
    def read_scores_dataframe(
            self,
            path: str,
            query: Optional[Query] = None) -> pl.DataFrame:
        return read_dataframe(path=path, query=query)
        
    def download_scores_by_date(
            self, 
            date: Optional[TIME] = None,
            overwrite: bool = False):

        date = util.parse_date(date or self.max_date)
        path = get_file_path(
            directory=self.raw_scores_by_date_dir, 
            date=date, 
            file_format=self.file_format,
        )
        if os.path.exists(path) and not overwrite:
            return

        download_scores_by_date(date=date, path=path)        

    def iter_daily_score_dataframes(
            self,
            query: Optional[Query] = None,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None,
            preserve_order: bool = True) -> Iterator[pl.DataFrame]:
        
        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)

        dates = util.iter_dates_in_range(min_date=min_date, max_date=max_date)
        if preserve_order:
            for date in dates:
                df = self.get_scores_dataframe_by_date(query=query, date=date)
                yield df
        else:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {}
                for date in dates:
                    future = executor.submit(self.get_scores_dataframe_by_date, query=query, date=date)
                    futures[future] = date
                
                futures = concurrent.futures.as_completed(futures)
                for future in futures:
                    df = future.result()
                    yield df

    def get_score_changelog_dataframe(
            self,
            query: Optional[Query] = None,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None,
            preserve_order: bool = True) -> pl.DataFrame:
        
        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)

        dfs = self.iter_score_changelog_dataframes(
            query=query,
            min_date=min_date, 
            max_date=max_date, 
            preserve_order=False,
        )
        df = pl.concat(dfs)
        if preserve_order:
            df = df.sort(by=DATE_AND_CVE)
        return df

    def iter_score_changelog_dataframes(
            self, 
            query: Optional[Query] = None, 
            min_date: Optional[TIME] = None, 
            max_date: Optional[TIME] = None,
            preserve_order: bool = True) -> Iterator[pl.DataFrame]:
        
        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)
        dates = util.iter_dates_in_range(min_date=min_date, max_date=max_date)

        dfs = map(lambda date: self.get_scores_dataframe_by_date(query=query, date=date), dates)

        if preserve_order:
            for (a, b) in util.iter_pairwise(dfs):
                df = get_diff(a, b)
                yield df
        else:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for (a, b) in util.iter_pairwise(dfs):
                    future = executor.submit(get_diff, a, b)
                    futures.append(future)
                
                futures = concurrent.futures.as_completed(futures)
                for future in futures:
                    df = future.result()
                    yield df

    def iter_score_changelog_entries(
            self, 
            query: Optional[Query] = None,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None,
            preserve_order: bool = True) -> Iterator[ScoreChangelogEntry]:
        
        dfs = self.iter_score_changelog_dataframes(
            query=query,
            min_date=min_date, 
            max_date=max_date, 
            preserve_order=preserve_order,
        )
        for df in dfs:
            for row in df.itertuples():
                yield ScoreChangelogEntry(
                    date=row.date,
                    cve=row.cve,
                    new_epss=row.new_epss,
                    old_epss=row.old_epss,
                    epss_change=row.epss_change,
                    epss_change_pct=row.epss_change_pct,
                    new_percentile=row.new_percentile,
                    old_percentile=row.old_percentile,
                    percentile_change=row.percentile_change,
                    percentile_change_pct=row.percentile_change_pct,
                )

    def get_score_by_cve_id(self, cve_id: str, date: Optional[TIME] = None) -> Optional[float]:
        query = get_query(cve_ids=[cve_id])
        df = self.get_scores_dataframe_by_date(query=query, date=date)
        return df['epss'].max()
    
    def get_score_range_dataframe(
            self,
            query: Optional[Query] = None,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None,
            preserve_order: bool = True) -> pl.DataFrame:
        
        df = self.get_score_changelog_dataframe(
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
        return df
    
    # TODO
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


def read_dataframe(path: str) -> pl.DataFrame:
    df = util.read_polars_dataframe(path)

    # Insert the `date` column if it's missing.
    if 'date' not in df.columns:
        date = util.get_date_from_filename(path)
        df = df.with_columns(
            date=date,
        )
    
    return df


def read_score_dataframes(
        paths: STRS, 
        preserve_order: bool = False) -> Iterator[pl.DataFrame]:
    
    f = functools.partial(read_dataframe)
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
        cve_ids: Optional[STRS] = None):
    
    date = util.parse_date(date)
    url = get_download_url(date=date)

    logger.info('Downloading scores for %s from %s to %s', date, url, path)
    response = requests.get(url, stream=True)
    response.raise_for_status()

    data = io.BytesIO(response.content)
    df = pl.read_csv(data, skip_rows=1)

    df.with_columns(
        date=date,
    )
    if cve_ids:
        df = filter_dataframe(df=df, cve_ids=cve_ids)

    util.write_polars_dataframe(df=df, path=path)
    logger.info('Downloaded scores for %s', date)


def get_file_path(
        date: TIME,
        directory: str, 
        file_format: str) -> str:

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
    logger.info('Resolving latest publication date for EPSS scores via %s', url)

    response = requests.head(url)
    location = response.headers["Location"]
    assert location is not None, "No Location header found"
    regex = r"(\d{4}-\d{2}-\d{2})"
    match = re.search(regex, location)
    assert match is not None, f"No date found in {location}"
    date = datetime.date.fromisoformat(match.group(1))

    logger.info('Latest publication date for EPSS scores is %s', date)
    return date


# TODO
def get_rolling_diff(dfs: Iterable[pl.DataFrame]) -> Iterator[Tuple[datetime.date, datetime.date, pl.DataFrame]]:
    """
    Assumptions:
    - The input dataframes are sorted chronologically.
    """
    for (a, b) in util.iter_pairwise(dfs):
        df = get_diff(a, b)
        yield df


# TODO
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
    df = df.sort(by=DATE_AND_CVE)
    
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
    return df
