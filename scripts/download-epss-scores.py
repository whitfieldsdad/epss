import concurrent.futures
import io
import os
import re
from typing import Iterable, Iterator, Optional, Union
import datetime
import requests
import pandas as pd
import logging

logger = logging.getLogger(__name__)


MIN_DATE = "2022-07-15"

TIME = Union[datetime.date, datetime.datetime, str, int, float]

CWD = os.getcwd()

CSV = 'csv'
CSV_GZ = 'csv.gz'
JSON = 'json'
JSONL = 'jsonl'
JSON_GZ = 'json.gz'
JSONL_GZ = 'jsonl.gz'
PARQUET = 'parquet'
PARQUET_GZ = 'parquet.gz'

OUTPUT_FORMATS = [CSV, CSV_GZ, JSON, JSONL, JSON_GZ, JSONL_GZ, PARQUET, PARQUET_GZ]

DEFAULT_OUTPUT_FORMAT = CSV_GZ

OVERWRITE = False


def download_scores_by_date(date: TIME, cve_ids: Optional[Iterable[str]] = None, output_dir: str = CWD, output_format: Optional[str] = OUTPUT_FORMATS, overwrite: bool = OVERWRITE):
    url = get_download_url(date)
    path = get_output_path_by_date(date=date, output_dir=output_dir, output_format=output_format)
    if not overwrite and os.path.exists(path):
        logger.debug(f"Skipping {path} because it already exists")
        return
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    response = requests.get(url, stream=True)
    response.raise_for_status()

    df = pd.read_csv(io.BytesIO(response.content), skiprows=1, compression="gzip")

    if cve_ids:
        df = df[df["cve_id"].isin(cve_ids)]

    compression = None
    if output_format in [CSV_GZ, JSON_GZ, JSONL_GZ, PARQUET_GZ]:
        compression = 'gzip'

    if output_format in [CSV, CSV_GZ]:
        df.to_csv(path, index=False, compression=compression)
    elif output_format in [JSON, JSON_GZ]:
        df.to_json(path, orient='records', compression=compression)
    elif output_format in [JSONL, JSONL_GZ]:
        df.to_json(path, orient='records', lines=True, compression=compression)
    elif output_format in [PARQUET, PARQUET_GZ]:
        df.to_parquet(path, index=False, compression=compression)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")


def download_scores_over_time(cve_ids: Optional[Iterable[str]] = None, min_date: Optional[TIME] = None, max_date: Optional[TIME] = None, output_dir: str = CWD, output_format: Optional[str] = OUTPUT_FORMATS, overwrite: bool = OVERWRITE):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for date in iter_dates_in_range(min_date=min_date, max_date=max_date):
            future = executor.submit(download_scores_by_date, date=date, cve_ids=cve_ids, output_dir=output_dir, output_format=output_format, overwrite=overwrite)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            future.result()


def get_output_path_by_date(date: TIME, output_dir: str = CWD, output_format: Optional[str] = OUTPUT_FORMATS) -> str:
    """
    $output_dir/by/date/$date.$output_format
    """
    output_dir = os.path.abspath(output_dir or CWD)
    output_format = output_format or DEFAULT_OUTPUT_FORMAT

    date = parse_date(date)
    return f'{output_dir}/{date.isoformat()}.{output_format}'


def get_download_url(date: Optional[TIME] = None) -> str:
    date = parse_date(date) if date else get_max_date()
    return f"https://epss.cyentia.com/epss_scores-{date.isoformat()}.csv.gz"


def get_output_format_from_path(path: str) -> str:
    for output_format in sorted(OUTPUT_FORMATS, key=len, reverse=True):
        ext = f'.{output_format}'
        if path.endswith(ext):
            return output_format
    raise ValueError(f"Could not determine output format from path: {path}")


def iter_dates_in_range(min_date: Optional[TIME] = None, max_date: Optional[TIME] = None) -> Iterator[datetime.date]:
    min_date = parse_date(min_date or MIN_DATE)
    max_date = parse_date(max_date or get_max_date())
    delta = max_date - min_date   # returns timedelta
    for i in range(delta.days + 1):
        day = min_date + datetime.timedelta(days=i)
        yield day


def get_max_date() -> datetime.date:
    url = "https://epss.cyentia.com/epss_scores-current.csv.gz"

    response = requests.head(url)
    location = response.headers["Location"]
    assert location is not None, "No Location header found"
    regex = r"(\d{4}-\d{2}-\d{2})"
    match = re.search(regex, location)
    assert match is not None, f"No date found in {location}"
    return datetime.date.fromisoformat(match.group(1))


def parse_date(date: TIME) -> datetime.date:
    if isinstance(date, datetime.date):
        return date
    elif isinstance(date, datetime.datetime):
        return date.date()
    elif isinstance(date, str):
        return datetime.datetime.strptime(date, "%Y-%m-%d").date()
    elif isinstance(date, (int, float)):
        return datetime.datetime.fromtimestamp(date).date()
    else:
        raise ValueError(f"Unsupported data format: {date}")


if __name__ == "__main__":
    import argparse

    def main(output_dir: str, output_format: str, cve_ids: Optional[Iterable[str]] = None, min_date: Optional[str] = None, max_date: Optional[str] = None, overwrite: Optional[bool] = OVERWRITE):
        download_scores_over_time(
            output_dir=output_dir, 
            output_format=output_format,
            overwrite=overwrite,
            cve_ids=cve_ids,
            min_date=min_date, 
            max_date=max_date, 
        )

    class SplitArgs(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest, values.split(','))

    def cli():
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

        parser = argparse.ArgumentParser("Idempotently download EPSS scores")
        parser.add_argument('--cve-ids', type=str, action=SplitArgs, help="CVE IDs to download scores for")
        parser.add_argument('--output-dir', '-o', type=str, default=CWD, help="Directory to write scores to")
        parser.add_argument('--output-format', '-f', type=str, default=DEFAULT_OUTPUT_FORMAT, choices=OUTPUT_FORMATS, help="Output format")
        parser.add_argument('--min-date', type=str, default=MIN_DATE, help="Minimum date to download scores for")
        parser.add_argument('--max-date', type=str, help='Maximum date to download scores for')
        parser.add_argument('--overwrite', action='store_true', help='Overwrite existing files', default=OVERWRITE)
        kwargs = vars(parser.parse_args()) 
        main(**kwargs)

    cli()
