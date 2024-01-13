import datetime 
import io
from typing import Optional, Iterable, Iterator, Tuple
from epss.util import *

import concurrent.futures
import logging
import os
import pandas as pd
import polars as pl
import re
import requests
import tqdm

from epss.constants import DEFAULT_FILE_FORMAT, MIN_DATE, OVERWRITE, TIME

logger = logging.getLogger(__name__)


def read_scores_from_file(
        path: str, 
        file_format: Optional[str] = DEFAULT_FILE_FORMAT, 
        drop_percentile_column: bool = False) -> pd.DataFrame:

    logger.debug('Reading EPSS scores from %s', path)
    file_format = parse_file_format(file_format) if file_format else None
    df = read_dataframe(path, file_format=file_format)

    # Add date column if it doesn't exist
    if 'date' not in df.columns:
        date = get_date_from_filename(path)
        df['date'] = date.isoformat()

    # Convert 'date' to datetime
    df['date'] = pd.to_datetime(df['date'])

    # Drop 'percentile' column if requested
    if drop_percentile_column:
        df = df.drop(columns=['percentile'])

    logger.debug('Read %d x %d dataframe from %s', len(df), len(df.columns), path)
    return df


def read_scores_from_dir(
        input_dir: str,
        drop_percentile_column: bool = False) -> Iterator[pd.DataFrame]:
    
    paths = iter_paths(input_dir)
    for path in paths:
        df = read_scores_from_file(path, drop_percentile_column=drop_percentile_column)
        yield df


def get_download_path(
        output_dir: str, 
        date: TIME, 
        file_format: Optional[str] = DEFAULT_FILE_FORMAT) -> str:

    date = parse_date(date)
    file_format = parse_file_format(file_format)
    return os.path.join(output_dir, f"{date.isoformat()}.{file_format}")


def download_scores_by_date(
        date: TIME,
        output_dir: str,
        cve_ids: Optional[Iterable[str]] = None,
        file_format: str = DEFAULT_FILE_FORMAT,
        overwrite: bool = OVERWRITE):
    
    date = parse_date(date)
    url = get_download_url(date)

    file_format = parse_file_format(file_format or DEFAULT_FILE_FORMAT)
    path = get_download_path(output_dir=output_dir, date=date, file_format=file_format)

    if not overwrite and os.path.exists(path):
        return
    
    logger.info('Downloading scores for %s from %s to %s', date, url, path)
    response = requests.get(url, stream=True)
    response.raise_for_status()

    df = pd.read_csv(io.BytesIO(response.content), skiprows=1, compression="gzip")
    df['date'] = date.isoformat()

    if cve_ids:
        df = df[df["cve_id"].isin(cve_ids)]

    write_dataframe(df=df, path=path, file_format=file_format)
    logger.info('Downloaded scores for %s', date)


def download_scores_over_time(
        output_dir: str,
        file_format: str = DEFAULT_FILE_FORMAT,
        overwrite: bool = OVERWRITE,
        cve_ids: Optional[Iterable[str]] = None,
        min_date: Optional[TIME] = None,
        max_date: Optional[TIME] = None):

    min_date = parse_date(min_date or get_min_date())
    max_date = parse_date(max_date or get_max_date())

    logger.info('Ensuring scores from %s to %s have been downloaded', min_date.isoformat(), max_date.isoformat())

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for date in iter_dates_in_range(min_date=min_date, max_date=max_date):
            path = get_download_path(output_dir=output_dir, date=date, file_format=file_format)
            if os.path.exists(path) and not overwrite:
                continue

            future = executor.submit(
                download_scores_by_date, 
                date=date, 
                output_dir=output_dir,
                file_format=file_format,
                overwrite=overwrite, 
                cve_ids=cve_ids,                 
            )
            futures.append(future)
        
        if futures:
            total = len(futures)
            logger.info('Downloading scores for %d dates', total)
            futures = concurrent.futures.as_completed(futures)
            futures = tqdm.tqdm(futures, total=total, desc="Downloading scores")
            for future in futures:
                future.result()
            logger.info('Downloaded scores for %d dates', len(futures))
        
        logger.info('All scores have been downloaded')


def partition_scores_by_date(
        df: pd.DataFrame,
        output_dir: str,
        file_format: Optional[str] = None):
    
    file_format = parse_file_format(file_format)
    for date, df in df.groupby('date'):
        df = df.sort_values(by=['cve'], ascending=False)

        path = get_download_path(output_dir=output_dir, date=date, file_format=file_format)
        write_dataframe(df=df, path=path, file_format=file_format)


def partition_scores_by_cve_id(
        df: pd.DataFrame,
        output_dir: str,
        file_format: Optional[str] = None):
    
    file_format = parse_file_format(file_format)
    for date, df in df.groupby('cve'):
        df = df.sort_values(by=['date'], ascending=False)

        path = get_download_path(output_dir=output_dir, date=date, file_format=file_format)
        write_dataframe(df=df, path=path, file_format=file_format)


def get_download_url(date: Optional[TIME] = None) -> str:
    date = parse_date(date) if date else get_max_date()
    return f"https://epss.cyentia.com/epss_scores-{date.isoformat()}.csv.gz"


def get_min_date() -> datetime.date:
    return parse_date(MIN_DATE)


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


def get_rolling_diff(dfs: Iterable[pd.DataFrame]) -> Iterator[Tuple[datetime.date, datetime.date, pd.DataFrame]]:
    for (a, b) in iter_pairwise(dfs):
        assert len(a['date'].unique()) == 1, f"Expected dataframe to contain a single date, not {a['date'].unique()}"
        assert len(b['date'].unique()) == 1, f"Expected dataframe to contain a single date, not {b['date'].unique()}"

        date_a = a['date'].max().to_pydatetime().date()
        date_b = b['date'].max().to_pydatetime().date()

        d = get_diff(a, b)
        yield date_a, date_b, d


def get_diff(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    min_date = d['date'].min()
    max_date = d['date'].max()

    logger.info('Calculating changes in EPSS scores between %s and %s', min_date, max_date)
    d = pd.concat([a, b]).sort_values(by=['date', 'cve'])
    
    assert min_date != max_date, f"Expected more than one date, not {min_date}"

    d = _get_diff(a, b)
    logger.info('Found %d changes in EPSS scores between %s and %s', len(d), min_date, max_date)
    return d


def _get_diff(df: pd.DataFrame) -> pd.DataFrame:
    # Lookup the previous EPSS score for each CVE.
    df['previous_epss'] = df.groupby('cve')['epss'].shift(1)
    df['previous_percentile'] = df.groupby('cve')['percentile'].shift(1)

    # Calculate the change in EPSS score.
    df['epss_change'] = (df['epss'] - df['previous_epss']).round(5)
    df['epss_change_pct'] = (((df['epss'] - df['previous_epss']) / df['previous_epss']) * 100).round(5)

    # Drop any rows where there was no previous EPSS score.
    df = df.dropna(subset=['previous_epss'])

    # Drop rows where there was no change in EPSS score.
    df = df[df['epss_change'] != 0]

    # Calculate the change in percentile.
    df['percentile_change'] = (df['percentile'] - df['previous_percentile']).round(5)
    df['percentile_change_pct'] = (((df['percentile'] - df['previous_percentile']) / df['previous_percentile']) * 100).round(5)
    
    # Reorder columns.
    columns = [
        'date', 
        'cve', 
        'epss', 
        'previous_epss', 
        'epss_change', 
        'epss_change_pct', 
        'percentile', 
        'previous_percentile', 
        'percentile_change', 
        'percentile_change_pct',
    ]
    df = df[columns]

    return df
