import concurrent.futures
import io
import os
import re
import sys
import tqdm
from typing import Iterable, Iterator, Optional, Union
import datetime
import requests
import pandas as pd
import logging
import click

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

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


def download_scores_by_date(
    date: TIME, 
    cve_ids: Optional[Iterable[str]] = None, 
    output_dir: str = CWD, 
    output_format: Optional[str] = OUTPUT_FORMATS, 
    overwrite: bool = OVERWRITE):

    url = get_download_url(date)
    path = get_output_path_by_date(date=date, output_dir=output_dir, output_format=output_format)
    if not overwrite and os.path.exists(path):
        logger.debug(f"Skipping {path} because it already exists")
        return
        
    response = requests.get(url, stream=True)
    response.raise_for_status()

    df = pd.read_csv(io.BytesIO(response.content), skiprows=1, compression="gzip")
    df['date'] = date.isoformat()

    if cve_ids:
        df = df[df["cve_id"].isin(cve_ids)]

    write_scores(df=df, path=path)


def read_scores(path: str) -> pd.DataFrame:
    fmt = get_file_format_from_path(path)

    compression = None
    if fmt in [CSV_GZ, JSON_GZ, JSONL_GZ]:
        compression = 'gzip'

    if fmt in [CSV, CSV_GZ]:
        df = pd.read_csv(path, compression=compression)
    elif fmt in [JSON, JSON_GZ]:
        df = pd.read_json(path, compression=compression)
    elif fmt in [JSONL, JSONL_GZ]:
        df = pd.read_json(path, lines=True, compression=compression)
    elif fmt in [PARQUET, PARQUET_GZ]:
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file format: {fmt}")
    
    # Add date column if it doesn't exist
    if 'date' not in df.columns:
        date = get_date_from_path(path)
        df['date'] = date.isoformat()
    
    # Use 'date' as date index
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df


def write_scores(df: pd.DataFrame, path: str):
    fmt = get_file_format_from_path(path)

    os.makedirs(os.path.dirname(path), exist_ok=True)

    compression = None
    if fmt in [CSV_GZ, JSON_GZ, JSONL_GZ, PARQUET_GZ]:
        compression = 'gzip'

    if fmt in [CSV, CSV_GZ]:
        df.to_csv(path, index=False, compression=compression)
    elif fmt in [JSON, JSON_GZ]:
        df.to_json(path, orient='records', compression=compression)
    elif fmt in [JSONL, JSONL_GZ]:
        df.to_json(path, orient='records', lines=True, compression=compression)
    elif fmt in [PARQUET, PARQUET_GZ]:
        df.to_parquet(path, index=False, compression=compression)
    else:
        raise ValueError(f"Unsupported output format: {fmt}")


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


def get_file_format_from_path(path: str) -> str:
    for output_format in sorted(OUTPUT_FORMATS, key=len, reverse=True):
        ext = f'.{output_format}'
        if path.endswith(ext):
            return output_format
    raise ValueError(f"Could not determine output format from path: {path}")


def iter_dates_in_range(min_date: Optional[TIME] = None, max_date: Optional[TIME] = None) -> Iterator[datetime.date]:
    min_date = parse_date(min_date or MIN_DATE)
    max_date = parse_date(max_date or get_max_date())
    delta = max_date - min_date
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


def reduce_scores(
    input_dir: str, 
    output_file: str,
    min_date: Optional[TIME] = None,
    max_date: Optional[TIME] = None):
    
    input_files = [os.path.join(input_dir, filename) for filename in os.listdir(input_dir)]
    input_files = sorted(filter_paths_by_timeframe(
        paths=input_files, 
        min_date=min_date,
        max_date=max_date,
    ))
    input_files = tqdm.tqdm(input_files, desc="Reducing scores")
    
    merged_df = None
    for path in input_files:
        df = read_scores(path)
        df.drop(columns=['percentile'], inplace=True)
        if merged_df is None:
            merged_df = df
            continue
        else:
            merged_df = pd.concat([merged_df, df])
            merged_df = merged_df.drop_duplicates(subset=['cve', 'epss'])
    
    df = merged_df.copy()
    del merged_df

    # Recalculate daily percentiles.
    df['percentile'] = df.groupby('date')['epss'].rank(pct=True)
    df['percentile'] = df['percentile'].round(2)

    # Calculate % change since last observation.
    df['epss_pct_change'] = df.groupby('cve')['epss'].pct_change() * 100

    # Convert index to column
    df = df.reset_index()
    df['date'] = df['date'].dt.date.astype(str)

    # Write to file
    write_scores(df=df, path=output_file)


def partition_scores(input_file: str, output_dir: str, output_format: Optional[str] = DEFAULT_OUTPUT_FORMAT, by: str = 'cve'):
    df = read_scores(input_file)    
    df = df.reset_index()
    df['date'] = df['date'].dt.date.astype(str)
 
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []

        if by == 'cve':
            for cve_id, cve_df in df.groupby('cve'):
                path = os.path.join(output_dir, f'{cve_id}.{output_format}')
                future = executor.submit(write_scores, cve_df, path)
                futures.append(future)

        elif by == 'date':
            for date, date_df in df.groupby('date'):
                path = os.path.join(output_dir, f'{date}.{output_format}')
                future = executor.submit(write_scores, date_df, path)
                futures.append(future)
        else:
            raise ValueError(f"Unsupported partitioning method: {by}")
    
        for future in tqdm.tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Partitioning scores"):
            future.result()


def filter_paths_by_timeframe(
        paths: Iterable[str], 
        min_date: Optional[TIME] = None, 
        max_date: Optional[TIME] = None) -> Iterator[str]:
    
    min_date = parse_date(min_date) if min_date else None
    max_date = parse_date(max_date) if max_date else None

    for path in paths:
        date = get_date_from_path(path)

        if min_date and date < min_date:
            continue

        if max_date and date > max_date:
            continue

        yield path


def get_date_from_path(path: str) -> datetime.date:
    filename = os.path.basename(path)
    return get_date_from_filename(filename)


def get_date_from_filename(filename: str) -> datetime.date:
    regex = r"(\d{4}-\d{2}-\d{2})"
    match = re.search(regex, filename)
    assert match is not None, f"No date found in {filename}"
    return datetime.date.fromisoformat(match.group(1))


if __name__ == "__main__":
    @click.group()
    def cli():
        pass

    @cli.command('download')
    @click.option('--date', required=False)
    @click.option('--min-date', required=False)
    @click.option('--max-date', required=False)
    @click.option('--all', 'download_all', is_flag=True, help='Download all scores')
    @click.option('--cve-ids', multiple=True, help='CVE IDs to download')
    @click.option('--output-dir', '-o', required=True, help='Output directory')
    @click.option('--output-format', default=DEFAULT_OUTPUT_FORMAT, type=click.Choice(OUTPUT_FORMATS), help='Output format')
    @click.option('--overwrite', is_flag=True, help='Overwrite existing files')
    def download_scores_command(
        date: Optional[str], 
        min_date: Optional[str], 
        max_date: Optional[str], 
        download_all: bool,
        cve_ids: Optional[Iterable[str]] = None, 
        output_dir: str = CWD, 
        output_format: Optional[str] = OUTPUT_FORMATS, 
        overwrite: bool = OVERWRITE):

        if download_all:
            if min_date or max_date:
                raise ValueError("Cannot specify --all with --min-date or --max-date")
            elif date:
                raise ValueError("Cannot specify --all with --date")

            min_date = MIN_DATE
            max_date = get_max_date()
        
        elif date:
            if min_date or max_date:
                raise ValueError("Cannot specify --date with --min-date or --max-date")
            elif download_all:
                raise ValueError("Cannot specify --date with --all")
            
            min_date = date
            max_date = date

        elif min_date or max_date:
            if date:
                raise ValueError("Cannot specify --min-date or --max-date with --date")

            if download_all:
                raise ValueError("Cannot specify --min-date or --max-date with --all")
        else:
            min_date = max_date = get_max_date()

        download_scores_over_time(
            cve_ids=cve_ids, 
            min_date=min_date, 
            max_date=max_date, 
            output_dir=output_dir, 
            output_format=output_format, 
            overwrite=overwrite,
        )

    @cli.command('reduce')
    @click.option('--input-dir', '-i', required=True, help='Input directory')
    @click.option('--output-file', '-o', required=True, help='Output file')
    @click.option('--min-date')
    @click.option('--max-date')
    def reduce_scores_command(
        input_dir: str, 
        output_file: str,
        min_date: str,
        max_date: str):
        """
        Merge and deduplicate scores
        """
        reduce_scores(
            input_dir=input_dir,
            output_file=output_file,
            min_date=min_date,
            max_date=max_date,
        )

    @cli.command('partition')
    @click.option('--input-file', '-i', required=True, help='Input file')
    @click.option('--output-dir', '-o', required=True, help='Output directory')
    @click.option('--output-format', default=DEFAULT_OUTPUT_FORMAT, type=click.Choice(OUTPUT_FORMATS), help='Output format')
    @click.option('--by', type=click.Choice(['cve', 'date']), default='cve')
    def partition_scores_command(
        input_file: str, 
        output_dir: str,
        output_format: str,
        by: str):
        """
        Partition scores in a file by CVE ID or date.
        """
        partition_scores(
            input_file=input_file,
            output_dir=output_dir,
            output_format=output_format,
            by=by,
        )

    cli()
