import collections
from dataclasses import dataclass
import os

from epss.constants import *
from typing import Any, Iterable, Iterator, Optional, Tuple
import logging
import pandas as pd
import concurrent.futures

import epss.epss as epss

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@dataclass()
class Query:
    cve_ids: Optional[Iterable[str]] = None,
    cpe_ids: Optional[Iterable[str]] = None
    vendors: Optional[Iterable[str]] = None
    products: Optional[Iterable[str]] = None
    min_score: Optional[float] = None
    max_score: Optional[float] = None
    min_percentile: Optional[float] = None
    max_percentile: Optional[float] = None
    min_peak_percentile: Optional[float] = None
    max_peak_percentile: Optional[float] = None
    min_peak_score: Optional[float] = None
    max_peak_score: Optional[float] = None
    min_date: Optional[TIME] = MIN_DATE
    max_date: Optional[TIME] = None
    days_ago: Optional[int] = None


@dataclass()
class Score:
    cve: str
    date: TIME
    epss: float
    percentile: float

    @property
    def cve_id(self) -> str:
        return self.cve
    
    @property
    def score(self) -> float:
        return self.epss


@dataclass()
class Client:
    """
    An opinionated client for downloading EPSS scores from https://epss.cyentia.com

    The client stores EPSS scores in Parquet format in a directory structure created under `workdir`.

    `workdir` has the following directory structure:

    - `workdir`
        - `date`
        - `cve`

    `workdir/date` contains EPSS scores partitioned by date in Parquet format (e.g. `2024-01-01.jsonl`).
    `workdir/cve` contains EPSS scores partitioned by CVE and date in Parquet format (e.g. `CVE-2014-0160.jsonl`).
    """
    workdir: str
    auto_update: bool = False

    def __post_init__(self):
        if self.auto_update:
            self.update()

    @property
    def min_date(self) -> datetime.date:
        return epss.parse_date(MIN_DATE)
    
    @property
    def max_date(self) -> datetime.date:
        return epss.get_max_date()
    
    def update(self):
        self.download_all()

    def download_all(self):
        epss.download_scores_over_time(
            output_dir=self.workdir,
            min_date=self.min_date,
            max_date=self.max_date,
        )

    def get_dataframe(self, query: Optional[Query] = None, sort_by_date: Optional[bool] = False) -> pd.DataFrame:
        logger.info('Reading dataframes')
        dfs = list(self.iter_dataframes(query, sort_by_date=sort_by_date))
        df = pd.concat(dfs)
        df = df.sort_values(by=['date', 'cve'], ascending=False)
        return df
    
    def iter_dataframes(
            self, 
            query: Optional[Query] = None,
            sort_by_date: Optional[bool] = False) -> Iterator[pd.DataFrame]:
        
        if self.auto_update:
            self.update()

        min_date = None
        max_date = None
        
        if query:
            min_date = epss.parse_date(query.min_date or MIN_DATE)
            max_date = epss.parse_date(query.max_date or self.max_date)

        paths_by_date = epss.iter_paths(
            root=self.workdir,
            min_date=min_date, 
            max_date=max_date,
            recursive=False,
            include_dirs=False,
        )
        paths_by_date = {epss.get_date_from_filename(path): path for path in paths_by_date}
        if sort_by_date:
            for date, path in sorted(paths_by_date.items(), reverse=True):
                df = self.read_dataframe(path=path, query=query)
                logger.debug('Resolved %d EPSS scores for %s', len(df), date)
                yield df
        else:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {}
                for date, path in paths_by_date.items():
                    future = executor.submit(self.read_dataframe, path=path, query=query)
                    futures[future] = date

                for future in concurrent.futures.as_completed(futures):
                    date = futures[future]
                    df = future.result()
                    logger.debug('Resolved %d EPSS scores for %s', len(df), date)
                    yield df

    def read_dataframe(self, path: str, query: Optional[Query] = None) -> pd.DataFrame:
        df = epss.read_scores_from_file(path=path)
        if query:
            df = self.filter_dataframe(df=df, query=query)
        return df

    def iter_scores(self, query: Optional[Query] = None) -> Iterator[Score]:
        for df in self.iter_dataframes(query):
            for _, row in df.iterrows():
                yield Score(
                    cve=row['cve'],
                    date=row['date'],
                    epss=row['epss'],
                    percentile=row['percentile'],
                )

    def filter_dataframes(self, dfs: pd.DataFrame, query: Optional[Query]) -> Iterator[pd.DataFrame]:
        for df in dfs:
            if query:
                df = self.filter_dataframe(df=df, query=query)
            yield df

    def filter_dataframe(self, df: pd.DataFrame, query: Optional[Query] = None) -> pd.DataFrame:
        if query:
            df = self._filter_dataframe(
                df=df,
                cve_ids=query.cve_ids,
                cpe_ids=query.cpe_ids,
                vendors=query.vendors,
                products=query.products,
                min_score=query.min_score,
                max_score=query.max_score,
                min_percentile=query.min_percentile,
                max_percentile=query.max_percentile,
                min_peak_percentile=query.min_peak_percentile,
                max_peak_percentile=query.max_peak_percentile,
                min_peak_score=query.min_peak_score,
                max_peak_score=query.max_peak_score,
                min_date=query.min_date,
                max_date=query.max_date,
                days_ago=query.days_ago,
            )
        return df

    def _filter_dataframe(
            self,
            df: pd.DataFrame,
            cve_ids: Optional[Iterable[str]] = None,
            cpe_ids: Optional[Iterable[str]] = None,
            vendors: Optional[Iterable[str]] = None,
            products: Optional[Iterable[str]] = None,
            min_score: Optional[float] = None,
            max_score: Optional[float] = None,
            min_percentile: Optional[float] = None,
            max_percentile: Optional[float] = None,
            min_peak_percentile: Optional[float] = None,
            max_peak_percentile: Optional[float] = None,
            min_peak_score: Optional[float] = None,
            max_peak_score: Optional[float] = None,
            min_date: Optional[TIME] = MIN_DATE,
            max_date: Optional[TIME] = None,
            days_ago: Optional[TIME] = None) -> pd.DataFrame:

            total_before = len(df)

            # Always perform time-boxing first.
            min_date = epss.parse_date(min_date)
            max_date = epss.parse_date(max_date) or self.max_date

            if days_ago:
                min_date = max_date - datetime.timedelta(days=days_ago)

            if min_date:
                min_date = pd.to_datetime(min_date)
                df = df[df['date'] >= min_date]

            if max_date:
                max_date = pd.to_datetime(max_date)
                df = df[df['date'] <= max_date]

            if any((cpe_ids, vendors, products)):
                raise NotImplementedError()
            
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

            if (min_peak_score or max_peak_score) is not None:
                raise NotImplementedError()

            if (min_peak_percentile or max_peak_percentile) is not None:
                raise NotImplementedError()
        
            total_after = len(df)
            if total_after < total_before:
                logger.debug("Selected %d/%d scores (%.5f)", total_after, total_before, (total_after / total_before) * 100)
            return df

    def get_query(
            self,
            cve_ids: Optional[Iterable[str]] = None,
            cpe_ids: Optional[Iterable[str]] = None,
            vendors: Optional[Iterable[str]] = None,
            products: Optional[Iterable[str]] = None,
            min_score: Optional[float] = None,
            max_score: Optional[float] = None,
            min_percentile: Optional[float] = None,
            max_percentile: Optional[float] = None,
            min_peak_percentile: Optional[float] = None,
            max_peak_percentile: Optional[float] = None,
            min_peak_score: Optional[float] = None,
            max_peak_score: Optional[float] = None,
            min_date: Optional[TIME] = MIN_DATE, 
            max_date: Optional[TIME] = None,
            days_ago: Optional[int] = None) -> Query:
        
        return Query(
            cve_ids=cve_ids,
            cpe_ids=cpe_ids,
            vendors=vendors,
            products=products,
            min_score=min_score,
            max_score=max_score,
            min_percentile=min_percentile,
            max_percentile=max_percentile,
            min_peak_percentile=min_peak_percentile,
            max_peak_percentile=max_peak_percentile,
            min_peak_score=min_peak_score,
            max_peak_score=max_peak_score,
            min_date=min_date,
            max_date=max_date,
            days_ago=days_ago,
        )


def get_date_range( 
        min_date: Optional[TIME] = None, 
        max_date: Optional[TIME] = None,
        queries: Optional[Iterable[Query]] = None) -> Tuple[Optional[datetime.date], Optional[datetime.date]]:
    
    dates = set()
    
    if min_date:
        dates.add(epss.parse_date(min_date))

    if max_date:
        dates.add(epss.parse_date(max_date))

    if queries:
        for query in queries:
            if query.min_date:
                dates.add(epss.parse_date(query.min_date))
            
            if query.max_date:
                dates.add(epss.parse_date(query.max_date))

    if not dates:
        return None, None
    
    min_date = min(dates)
    max_date = max(dates)
    return min_date, max_date
