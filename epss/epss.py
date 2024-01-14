import abc
from dataclasses import dataclass
import shutil
import sys
from epss import util
from epss.constants import CACHE_DIR, DEFAULT_FILE_FORMAT, MIN_DATE, OVERWRITE, PARQUET, STRS, TIME
from typing import Any, Dict, List, Optional, Iterable, Iterator, Set, Tuple
import concurrent.futures
import datetime
import functools 
import io
import logging
import os
import pandas as pd
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
        shutil.rmtree(self.raw_scores_by_date_dir)

    def clear_score_changelogs_by_date_dir(self):
        shutil.rmtree(self.score_changelogs_by_date_dir)

    @property
    def cve_ids(self) -> Set[str]:
        df = self.get_scores_dataframe(date=self.max_date)
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
        self.download_scores(min_date=min_date, max_date=max_date)
        self.build_score_changelogs()
    
    def build_score_changelogs(self, min_date: Optional[TIME] = None, max_date: Optional[TIME] = None):
        for df in self.iter_score_changelog_dataframes(min_date=min_date, max_date=max_date):
            date = df['date'].iloc[0]
            path = get_file_path(
                directory=self.score_changelogs_by_date_dir, 
                date=date, 
                file_format=self.file_format,
            )
            util.write_dataframe(df=df, path=path)

    def download_scores(self, min_date: Optional[TIME] = None, max_date: Optional[TIME] = None):
        """
        Download daily sets of EPSS scores over the given date range.
        """
        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {}
            for date in util.iter_dates_in_range(min_date=min_date, max_date=max_date):
                path = get_file_path(directory=self.raw_scores_by_date_dir, date=date, file_format=self.file_format)
                if os.path.exists(path):
                    continue

                future = executor.submit(
                    download_scores_by_date, 
                    date=date, 
                    path=path,
                )
                futures[future] = date
            
            if futures:
                total = len(futures)

                dates = set(futures.values())
                min_date = min(dates)
                max_date = max(dates)

                if min_date == max_date:
                    logger.debug('Downloading scores for %s', min_date)
                else:
                    logger.debug('Downloading scores for %d dates (min: %s, max: %s)', total, min_date, max_date)

                futures = concurrent.futures.as_completed(futures)
                for future in futures:
                    future.result()
                logger.debug('Downloaded scores for %d dates', len(futures))        

    def get_scores_dataframe(
            self,
            query: Optional[Query] = None,
            date: Optional[TIME] = None) -> pd.DataFrame:
        
        date = util.parse_date(date or self.max_date)
        path = get_file_path(directory=self.raw_scores_by_date_dir, date=date, file_format=self.file_format)
        if not os.path.exists(path):
            self.download_scores_by_date(date=date)

        df = read_scores_dataframe(path)
        if query:
            df = filter_dataframe_with_query(df=df, query=query)
        return df
    
    def read_scores_dataframe(
            self,
            path: str,
            query: Optional[Query] = None) -> pd.DataFrame:
        return read_scores_dataframe(path=path, query=query)

    def get_score_by_cve_id(self, cve_id: str, date: Optional[TIME] = None) -> Optional[float]:
        query = get_query(cve_ids=[cve_id])
        df = self.get_scores_dataframe(query=query, date=date)
        return df['epss'].max()
        
    def download_scores_by_date(self, date: Optional[TIME] = None):
        date = util.parse_date(date or self.max_date)
        path = get_file_path(
            directory=self.raw_scores_by_date_dir, 
            date=date, 
            file_format=self.file_format,
        )
        download_scores_by_date(date=date, path=path)        

    def iter_daily_scores_dataframes(
            self,
            query: Optional[Query] = None,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None,
            preserve_order: bool = True) -> Iterator[pd.DataFrame]:
        
        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)

        dates = util.iter_dates_in_range(min_date=min_date, max_date=max_date)
        if preserve_order:
            for date in dates:
                df = self.get_scores_dataframe(query=query, date=date)
                yield df
        else:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {}
                for date in dates:
                    future = executor.submit(self.get_scores_dataframe, query=query, date=date)
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
            preserve_order: bool = True) -> pd.DataFrame:
        
        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)

        dfs = self.iter_score_changelog_dataframes(
            query=query,
            min_date=min_date, 
            max_date=max_date, 
            preserve_order=False,
        )
        df = pd.concat(dfs)
        if preserve_order:
            df = df.sort_values(by=['date', 'cve'])
        return df

    # TODO
    def iter_score_changelog_dataframes(
            self, 
            query: Optional[Query] = None, 
            min_date: Optional[TIME] = None, 
            max_date: Optional[TIME] = None,
            preserve_order: bool = True) -> Iterator[pd.DataFrame]:
        
        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)
        dates = util.iter_dates_in_range(min_date=min_date, max_date=max_date)

        dfs = map(lambda date: self.get_scores_dataframe(query=query, date=date), dates)

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

    # TODO
    def get_min_score_by_cve_id(
            self,
            cve_id: str,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None) -> Optional[float]:
        raise NotImplementedError()
    
    # TODO
    def get_max_score_by_cve_id(self,
            cve_id: str,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None) -> Optional[float]:
        raise NotImplementedError()



def read_scores_dataframe(path: str, query: Optional[Query] = None) -> pd.DataFrame:    
    df = util.read_dataframe(path)

    # Add date column if it doesn't exist
    if 'date' not in df.columns:
        date = util.get_date_from_filename(path)
        df['date'] = date.isoformat()

    # Convert 'date' to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Optionally filter the contents of the dataframe.
    if query:
        df = filter_dataframe_with_query(df=df, query=query)
    return df


def read_score_dataframes(
        paths: STRS, 
        query: Optional[Query] = None,
        preserve_order: bool = False) -> Iterator[pd.DataFrame]:
    
    f = functools.partial(read_scores_dataframe, query=query)
    if preserve_order:
        yield from map(f, paths)
    else:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            yield from executor.map(f, paths)


def filter_dataframes_with_query(dfs: pd.DataFrame, query: Optional[Query]) -> Iterator[pd.DataFrame]:
        for df in dfs:
            if query:
                df = filter_dataframe_with_query(df=df, query=query)
            yield df


def filter_dataframe_with_query(df: pd.DataFrame, query: Optional[Query] = None) -> pd.DataFrame:
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
        dfs: Iterable[pd.DataFrame],
        cve_ids: Optional[STRS] = None,
        min_score: Optional[float] = None,
        max_score: Optional[float] = None,
        min_percentile: Optional[float] = None,
        max_percentile: Optional[float] = None,
        min_date: Optional[TIME] = MIN_DATE,
        max_date: Optional[TIME] = None,
        days_ago: Optional[TIME] = None) -> Iterator[pd.DataFrame]:
    
    f = functools.partial(
        filter_dataframe,
        cve_ids=cve_ids,
        min_score=min_score,
        max_score=max_score,
        min_percentile=min_percentile,
        max_percentile=max_percentile,
        min_date=min_date,
        max_date=max_date,
        days_ago=days_ago,
    )
    yield from map(f, dfs)


def filter_dataframe(
        df: pd.DataFrame,
        cve_ids: Optional[STRS] = None,
        min_score: Optional[float] = None,
        max_score: Optional[float] = None,
        min_percentile: Optional[float] = None,
        max_percentile: Optional[float] = None,
        min_date: Optional[TIME] = None,
        max_date: Optional[TIME] = None,
        days_ago: Optional[TIME] = None) -> pd.DataFrame:

        total_before = len(df)
        min_date = util.parse_date(min_date)
        max_date = util.parse_date(max_date) or get_max_date()

        if days_ago:
            min_date = max_date - datetime.timedelta(days=days_ago)

        if min_date or max_date:
            if df['date'].dtype != 'datetime64[ns]':
                logger.warning("Setting `date` column to datetime64[ns] - was %s", df['date'].dtype)
                df['date'] = pd.to_datetime(df['date'])
            
            if min_date:
                min_date = pd.to_datetime(min_date)
                df = df[df['date'] >= min_date]

            if max_date:
                max_date = pd.to_datetime(max_date)
                df = df[df['date'] <= max_date]

        if cve_ids:
            df = df[df['cve'].isin(cve_ids)]

        if min_score:
            df = df[df['epss'] >= min_score]

        if max_score:
            df = df[df['epss'] <= max_score]
        
        if min_percentile:
            df = df[df['percentile'] >= min_percentile]

        if max_percentile:
            df = df[df['percentile'] <= max_percentile]

        total_after = len(df)
        if total_after < total_before:
            logger.debug("Selected %d/%d scores (%.5f)", total_after, total_before, (total_after / total_before) * 100)
        return df


def download_scores_over_time(
        output_dir: str,
        file_format: str = DEFAULT_FILE_FORMAT,
        cve_ids: Optional[STRS] = None,
        min_date: Optional[TIME] = None,
        max_date: Optional[TIME] = None):

    min_date = util.parse_date(min_date or get_min_date())
    max_date = util.parse_date(max_date or get_max_date())

    logger.debug('Ensuring scores from %s to %s have been downloaded', min_date.isoformat(), max_date.isoformat())

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
            total = len(futures)
            logger.debug('Downloading scores for %d dates', total)
            futures = concurrent.futures.as_completed(futures)
            for future in futures:
                future.result()
            logger.debug('Downloaded scores for %d dates', len(futures))
        
        logger.debug('All scores have been downloaded')


def download_scores_by_date(
        date: TIME,
        path: str,
        cve_ids: Optional[STRS] = None):
    
    date = util.parse_date(date)
    url = get_download_url(date=date)

    logger.info('Downloading scores for %s from %s to %s', date, url, path)
    response = requests.get(url, stream=True)
    response.raise_for_status()

    df = pd.read_csv(io.BytesIO(response.content), skiprows=1, compression="gzip")
    df['date'] = date.isoformat()

    if cve_ids:
        df = df[df["cve_id"].isin(cve_ids)]

    util.write_dataframe(df=df, path=path)
    logger.debug('Downloaded scores for %s', date)


def get_file_path(
        date: TIME,
        directory: str, 
        file_format: str) -> str:

    date = util.parse_date(date)
    file_format = util.parse_file_format(file_format)
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


def get_rolling_diff(dfs: Iterable[pd.DataFrame]) -> Iterator[Tuple[datetime.date, datetime.date, pd.DataFrame]]:
    for (a, b) in util.iter_pairwise(dfs):
        assert len(a['date'].unique()) == 1, f"Expected dataframe to contain a single date, not {a['date'].unique()}"
        assert len(b['date'].unique()) == 1, f"Expected dataframe to contain a single date, not {b['date'].unique()}"

        date_a = a['date'].max().to_pydatetime().date()
        date_b = b['date'].max().to_pydatetime().date()

        d = get_diff(a, b)
        yield date_a, date_b, d


def get_file_diff(a: str, b: str, query: Optional[Query] = None) -> pd.DataFrame:
    a = read_scores_dataframe(a, query=query)
    b = read_scores_dataframe(b, query=query)
    return get_diff(a, b)


def get_diff(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    df = pd.concat([a, b]).sort_values(by=['date', 'cve'])
    min_date = df['date'].min().date()
    max_date = df['date'].max().date()

    assert min_date != max_date, f"Expected more than one date, not {min_date}"
    df = _get_diff(df)
    logger.info('Found %d changes in EPSS scores between %s and %s', len(df), min_date.isoformat(), max_date.isoformat())
    return df


def _get_diff(df: pd.DataFrame) -> pd.DataFrame:
    # Rename columns
    df = df.rename(columns={
        'epss': 'new_epss',
        'percentile': 'new_percentile',
    })

    # Lookup the previous EPSS score for each CVE.
    df['old_epss'] = df.groupby('cve')['new_epss'].shift(1)
    df['old_percentile'] = df.groupby('cve')['new_percentile'].shift(1)

    # Calculate the change in EPSS score.
    df['epss_change'] = (df['new_epss'] - df['old_epss']).round(5)
    df['epss_change_pct'] = (((df['new_epss'] - df['old_epss']) / df['old_epss']) * 100).round(5)

    # Drop any rows where there was no previous EPSS score.
    df = df.dropna(subset=['old_epss'])

    # Drop rows where there was no change in EPSS score.
    df = df[df['epss_change'] != 0]

    # Calculate the change in percentile.
    df['percentile_change'] = (df['new_percentile'] - df['old_percentile']).round(5)
    df['percentile_change_pct'] = (((df['new_percentile'] - df['old_percentile']) / df['old_percentile']) * 100).round(5)
    
    # Reorder columns.
    columns = [
        'date', 
        'cve', 
        'old_epss', 
        'new_epss', 
        'epss_change', 
        'epss_change_pct', 
        'old_percentile', 
        'new_percentile', 
        'percentile_change', 
        'percentile_change_pct',
    ]
    df = df[columns]

    return df
