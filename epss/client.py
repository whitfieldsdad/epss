from dataclasses import dataclass
import datetime
import functools
import io
import itertools
import os
import re
import sys
from typing import Iterable, Iterator, Optional, Tuple, Union
from epss import util
from epss.constants import DEFAULT_FILE_FORMAT, MIN_DATE, TIME
import logging
import polars as pl
import requests
import concurrent.futures

logger = logging.getLogger(__name__)


@dataclass()
class Client:
    file_format: str = DEFAULT_FILE_FORMAT
    verify_tls: bool = True

    @property
    def min_date(self) -> datetime.date:
        return self.get_min_date()
    
    @property
    def max_date(self) -> datetime.date:
        return self.get_max_date()

    def get_min_date(self) -> datetime.date:
        return get_min_date()

    def get_max_date(self) -> datetime.date:
        return get_max_date(verify_tls=self.verify_tls)
    
    def get_date_range(self) -> Tuple[datetime.date, datetime.date]:
        return get_date_range(verify_tls=self.verify_tls)

    def get_score_dataframe(
            self, 
            workdir: str, 
            min_date: Optional[TIME] = None, 
            max_date: Optional[TIME] = None,
            sparse: bool = True) -> pl.DataFrame:
        
        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)
        dates = tuple(util.iter_dates_in_range(min_date, max_date))
        logger.info("Reading scores published between %s and %s (%d dates)", min_date.isoformat(), max_date.isoformat(), len(dates))

        resolver = functools.partial(
            self.get_score_dataframe_by_date,
            workdir=workdir,
        )
        with concurrent.futures.ThreadPoolExecutor() as executor:
            dfs = executor.map(lambda d: resolver(date=d), dates)
            if not sparse:
                return pl.concat(dfs)

            first = next(dfs)
            changes = executor.map(lambda e: get_changed_scores(*e), util.iter_pairwise(dfs))
            dfs = itertools.chain([first], changes)
            df = pl.concat(dfs)
            return df

    def get_score_dataframe_by_date(
            self,
            workdir: str, 
            date: Optional[TIME] = None) -> pl.DataFrame:
        
        date = util.parse_date(date or self.max_date)
        path = get_file_path(
            workdir=workdir,
            file_format=self.file_format,
            key=date.isoformat(),
        )
        if not os.path.exists(path):
            self.download_scores_by_date(workdir=workdir, date=date)
            assert os.path.exists(path), "Scores unexpectedly not downloaded"
        
        return read_dataframe(path)

    def download_scores(
            self,
            workdir: str,
            min_date: Optional[TIME] = None,
            max_date: Optional[TIME] = None):
        
        min_date = util.parse_date(min_date or self.min_date)
        max_date = util.parse_date(max_date or self.max_date)
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for date in util.iter_dates_in_range(min_date, max_date):
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
                    futures.append(future)
            
            if futures:
                total = len(futures)
                logger.info('Downloading scores for %s - %s (%d dates)', min_date.isoformat(), max_date.isoformat(), total)
                for future in concurrent.futures.as_completed(futures):
                    future.result()
            
            logger.info("All scores have been downloaded")

    def download_scores_by_date(self, workdir: str, date: TIME):
        date = util.parse_date(date)
        path = get_file_path(
            workdir=workdir, 
            file_format=self.file_format, 
            key=date.isoformat(),
        )
        if os.path.exists(path):
            logger.debug("Scores for %s have already been downloaded: %s", date.isoformat(), path)
            return
        
        url = get_download_url(date)
        logger.info('Downloading scores for %s: %s -> %s', date.isoformat(), url, path)

        response = requests.get(url, verify=self.verify_tls, stream=True)
        response.raise_for_status()

        data = io.BytesIO(response.content)
        df = pl.read_csv(data, skip_rows=1)
        df.with_columns(
            date=date,
        )
        util.write_dataframe(df=df, path=path)


def get_file_path(workdir: str, file_format: str, key: Union[datetime.date, str]) -> str:
    if isinstance(key, datetime.date):
        key = key.isoformat()
    return os.path.join(workdir, f'{key}.{file_format}')


def get_download_url(date: Optional[TIME] = None) -> str:
    date = util.parse_date(date) if date else get_max_date()
    return f"https://epss.cyentia.com/epss_scores-{date.isoformat()}.csv.gz"


def get_min_date() -> datetime.date:
    return util.parse_date(MIN_DATE)


def get_max_date(verify_tls: bool = True) -> datetime.date:
    url = "https://epss.cyentia.com/epss_scores-current.csv.gz"
    logger.info("Resolving latest publication date for EPSS scores")

    response = requests.head(url, verify=verify_tls)
    location = response.headers["Location"]
    assert location is not None, "No Location header found"
    regex = r"(\d{4}-\d{2}-\d{2})"
    match = re.search(regex, location)
    assert match is not None, f"No date found in {location}"
    date = datetime.date.fromisoformat(match.group(1))

    logger.info(f'EPSS scores were last published on {date.isoformat()}')
    return date


def get_date_range(verify_tls: bool = True) -> Tuple[datetime.date, datetime.date]:
    return get_min_date(), get_max_date(verify_tls=verify_tls)


def get_changed_scores(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
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


def read_dataframe(path: str) -> pl.DataFrame:
    df = util.read_dataframe(path)

    # Insert the `date` column if it's missing.
    if 'date' not in df.columns:
        date = util.get_date_from_filename(path)
        assert date is not None, "Date not found in filename"
        df = df.with_columns(
            date=date,
        )

    return df