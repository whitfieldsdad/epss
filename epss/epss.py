import concurrent.futures
import io
import os
import re
import sys
import tqdm
from typing import Iterable, Iterator, Optional, Tuple
import datetime
import requests
import pandas as pd
import logging
from epss import util
from epss.constants import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def download_scores_by_date(
    date: TIME, 
    path: str,
    cve_ids: Optional[Iterable[str]] = None, 
    file_format: Optional[str] = None, 
    overwrite: bool = OVERWRITE):

    date = util.parse_date(date)
    file_format = util.parse_file_format(file_format) if file_format else None

    url = get_download_url(date)
    if not overwrite and os.path.exists(path):
        return
    
    logger.debug('Downloading scores for %s from %s to %s', date, url, path)
    response = requests.get(url, stream=True)
    response.raise_for_status()

    df = pd.read_csv(io.BytesIO(response.content), skiprows=1, compression="gzip")
    df['date'] = date.isoformat()

    if cve_ids:
        df = df[df["cve_id"].isin(cve_ids)]

    util.write_dataframe(df=df, path=path, file_format=file_format)
    logger.debug('Downloaded scores for %s', date)


def read_scores(path: str, file_format: Optional[str] = None, drop_percentile_column: bool = False) -> pd.DataFrame:
    file_format = util.parse_file_format(file_format) if file_format else None
    df = util.read_dataframe(path, file_format=file_format)

    # Add date column if it doesn't exist
    if 'date' not in df.columns:
        date = util.get_date_from_filename(path)
        df['date'] = date.isoformat()

    # Convert 'date' to datetime
    df['date'] = pd.to_datetime(df['date'])

    # Drop 'percentile' column if requested
    if drop_percentile_column:
        df = df.drop(columns=['percentile'])

    return df


def download_scores_over_time(
        output_dir: str, 
        cve_ids: Optional[Iterable[str]] = None, 
        min_date: Optional[TIME] = None, 
        max_date: Optional[TIME] = None, 
        file_format: Optional[str] = FILE_FORMATS, 
        overwrite: bool = OVERWRITE):

    min_date = util.parse_date(min_date or get_min_date())
    max_date = util.parse_date(max_date or get_max_date())

    logger.debug('Ensuring scores from %s to %s have been downloaded', min_date.isoformat(), max_date.isoformat())

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for date in util.iter_dates_in_range(min_date=min_date, max_date=max_date):
            path = get_download_path(output_dir=output_dir, date=date, file_format=file_format)
            if os.path.exists(path) and not overwrite:
                continue

            future = executor.submit(
                download_scores_by_date, 
                date=date, 
                path=path,
                file_format=file_format,
                overwrite=overwrite, 
                cve_ids=cve_ids,                 
            )
            futures.append(future)
        
        if futures:
            total = len(futures)
            logger.debug('Downloading scores for %d dates', total)
            futures = concurrent.futures.as_completed(futures)
            futures = tqdm.tqdm(futures, total=total, desc="Downloading scores")
            for future in futures:
                future.result()
            logger.debug('Downloaded scores for %d dates', len(futures))
        
        logger.debug('All scores have been downloaded')


def get_download_path(
        output_dir: str, 
        date: TIME, 
        file_format: Optional[str] = DEFAULT_FILE_FORMAT) -> str:

    date = util.parse_date(date)
    file_format = util.parse_file_format(file_format)
    return os.path.join(output_dir, f"{date.isoformat()}.{file_format}")


def get_download_url(date: Optional[TIME] = None) -> str:
    date = util.parse_date(date) if date else get_max_date()
    return f"https://epss.cyentia.com/epss_scores-{date.isoformat()}.csv.gz"


def get_min_date() -> datetime.date:
    return util.parse_date(MIN_DATE)


def get_max_date() -> datetime.date:
    url = "https://epss.cyentia.com/epss_scores-current.csv.gz"

    response = requests.head(url)
    location = response.headers["Location"]
    assert location is not None, "No Location header found"
    regex = r"(\d{4}-\d{2}-\d{2})"
    match = re.search(regex, location)
    assert match is not None, f"No date found in {location}"
    return datetime.date.fromisoformat(match.group(1))


def rolling_diff_from_files(paths: Iterable[str]) -> Iterator[Tuple[datetime.date, datetime.date, pd.DataFrame]]:
    paths = util.sort_file_paths_by_date(paths)
    yield from rolling_diff(map(read_scores, paths))


def rolling_diff(dfs: Iterable[pd.DataFrame]) -> Iterator[Tuple[datetime.date, datetime.date, pd.DataFrame]]:
    for (a, b) in util.iter_pairwise(dfs):
        assert len(a['date'].unique()) == 1, f"Expected dataframe to contain a single date, not {a['date'].unique()}"
        assert len(b['date'].unique()) == 1, f"Expected dataframe to contain a single date, not {b['date'].unique()}"

        date_a = a['date'].max().to_pydatetime().date()
        date_b = b['date'].max().to_pydatetime().date()

        d = diff(a, b)
        yield date_a, date_b, d


def diff(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    d = pd.concat([a, b]).sort_values(by=['date', 'cve'])

    # Lookup the previous EPSS score for each CVE.
    d['previous_epss'] = d.groupby('cve')['epss'].shift(1)
    d['previous_percentile'] = d.groupby('cve')['percentile'].shift(1)

    # Calculate the change in EPSS score.
    d['epss_change'] = (d['epss'] - d['previous_epss']).round(5)
    d['epss_change_pct'] = (((d['epss'] - d['previous_epss']) / d['previous_epss']) * 100).round(5)

    # Drop any rows where there was no previous EPSS score.
    d = d.dropna(subset=['previous_epss'])

    # Drop rows where there was no change in EPSS score.
    d = d[d['epss_change'] != 0]

    # Calculate the change in percentile.
    d['percentile_change'] = (d['percentile'] - d['previous_percentile']).round(5)
    d['percentile_change_pct'] = (((d['percentile'] - d['previous_percentile']) / d['previous_percentile']) * 100).round(5)
    
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
    d = d[columns]

    return d
