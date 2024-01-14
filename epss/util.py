import itertools
from typing import Iterable, List, Optional, Iterator
import datetime
from epss.constants import TIME, CSV, CSV_GZ, JSON, JSONL, JSON_GZ, JSONL_GZ, PARQUET, FILE_FORMATS

import polars as pl
import logging
import os
import re

logger = logging.getLogger(__name__)


def read_polars_dataframe(path: str, file_format: Optional[str] = None) -> pl.DataFrame:
    if not file_format:
        file_format = get_file_format_from_path(path)

    compression = None
    if file_format in [CSV_GZ, JSON_GZ, JSONL_GZ]:
        compression = 'gzip'

    if file_format in [CSV, CSV_GZ]:
        df = pl.read_csv(path, compression=compression)
    elif file_format in [JSON, JSON_GZ]:
        df = pl.read_json(path, compression=compression)
    elif file_format in [JSONL, JSONL_GZ]:
        df = pl.read_json(path, lines=True, compression=compression)
    elif file_format in [PARQUET]:
        df = pl.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    return df


def write_polars_dataframe(df: pl.DataFrame, path: str, file_format: Optional[str] = None):
    path = realpath(path)
    if not file_format:
        file_format = get_file_format_from_path(path)

    compression = None
    if file_format in [CSV_GZ, JSON_GZ, JSONL_GZ]:
        compression = 'gzip'

    logger.debug('Writing %d x %d dataframe to %s (columns: %s)', len(df), len(df.columns), path, tuple(df.columns))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if file_format in [CSV, CSV_GZ]:
        df.write_csv(path, compression=compression)

    elif file_format in [JSON, JSON_GZ]:
        df.write_json(path, compression=compression, row_oriented=True)
    elif file_format in [JSONL, JSONL_GZ]:
        df.write_ndjson(path, compression=compression)
    elif file_format in [PARQUET]:
        df.write_parquet(path)
    else:
        raise ValueError(f"Unsupported output format: {file_format}")
    
    logger.debug('Wrote dataframe to %s', path)




def get_file_format_from_path(path: str) -> str:
    for output_format in sorted(FILE_FORMATS, key=len, reverse=True):
        ext = f'.{output_format}'
        if path.endswith(ext):
            return output_format
    raise ValueError(f"Could not determine output format from path: {path}")


def get_date_from_filename(filename: str) -> Optional[datetime.date]:
    filename = os.path.basename(filename)
    regex = r"(\d{4}-\d{2}-\d{2})"
    match = re.search(regex, filename)
    if match is not None:
        return datetime.date.fromisoformat(match.group(1))


def realpath(path: str) -> str:
    for f in [os.path.expandvars, os.path.expanduser, os.path.realpath]:
        path = f(path)
    return path


def iter_dates_in_range(min_date: TIME, max_date: TIME) -> Iterator[datetime.date]:
    min_date = parse_date(min_date)
    max_date = parse_date(max_date)
    delta = max_date - min_date
    for i in range(delta.days + 1):
        day = min_date + datetime.timedelta(days=i)
        yield day


def parse_date(d: Optional[TIME]) -> Optional[datetime.date]:
    if d is not None:
        if isinstance(d, datetime.datetime):
            return d.date()
        elif isinstance(d, datetime.date):
            return d
        elif isinstance(d, str):
            return datetime.datetime.strptime(d, "%Y-%m-%d").date()
        elif isinstance(d, (int, float)):
            return datetime.datetime.fromtimestamp(d).date()
        else:
            raise ValueError(f"Unsupported data format: {d}")


def parse_datetime(t: Optional[TIME]) -> datetime.datetime:
    if t is not None:
        if isinstance(t, datetime.datetime):
            return t
        elif isinstance(t, datetime.date):
            return datetime.datetime.combine(t, datetime.time())
        elif isinstance(t, str):
            return datetime.datetime.fromisoformat(t)
        elif isinstance(t, (int, float)):
            return datetime.datetime.fromtimestamp(t)
        else:
            raise ValueError(f"Unsupported data format: {t}")


def iter_paths(
        root: str, 
        recursive: bool = False, 
        min_date: Optional[TIME] = None, 
        max_date: Optional[TIME] = None,
        include_dirs: Optional[bool] = True) -> Iterator[str]:

    paths = _iter_paths(root, recursive=recursive)
    if not include_dirs:
        paths = filter(lambda p: os.path.isdir(p) is False, paths)

    if min_date or max_date:
        paths = filter_paths_by_timeframe(paths, min_date=min_date, max_date=max_date)
    yield from paths


def _iter_paths(root: str, recursive: bool = False):
    root = realpath(root)
    if not recursive:
        for filename in os.listdir(root):
            yield os.path.join(root, filename)
    else:
        for directory, _, files in os.walk(root):
            for filename in files:
                yield os.path.join(directory, filename)


def filter_paths_by_timeframe(
        paths: Iterable[str], 
        min_date: Optional[TIME] = None, 
        max_date: Optional[TIME] = None) -> Iterator[str]:
    
    min_date = parse_date(min_date)
    max_date = parse_date(max_date)

    for path in paths:
        if min_date or max_date:
            date = get_date_from_filename(path)
            if min_date and date < min_date:
                continue
            if max_date and date > max_date:
                continue
        yield path


def sort_paths_by_date(paths: Iterable[str]) -> List[str]:
    return sorted(paths, key=lambda p: get_date_from_filename(p))


def iter_pairwise(iterable: Iterable) -> Iterator:
    """
    Iterate over pairs of items in an iterable.

    Example:

    >>> list(pairwise([1]))
    []
    >>> list(pairwise([1,2]))
    [(1, 2)]
    >>> list(pairwise([1,2,3]))
    [(1, 2), (2, 3)]
    """
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def iter_chunks(iterable: Iterable, chunk_size: int) -> Iterator:
    """
    Iterate over chunks of items in an iterable.

    Example:

    >>> list(iter_chunks([1,2,3,4,5,6,7], 3))
    [[1, 2, 3], [4, 5, 6], [7]]
    """
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, chunk_size))
        if not chunk:
            break
        yield chunk