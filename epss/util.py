import itertools
from typing import Dict, Iterable, List, Optional, Iterator, Union
import datetime
from epss.constants import DEFAULT_FILE_FORMAT, TIME, CSV, CSV_GZ, JSON, JSONL, JSON_GZ, JSONL_GZ, PARQUET, FILE_FORMATS

import polars as pl
import concurrent.futures
import logging
import os
import re

logger = logging.getLogger(__name__)


def read_dataframe(path: str, file_format: Optional[str] = None) -> pl.DataFrame:
    if not file_format:
        file_format = get_file_format_from_path(path)

    if file_format in [CSV_GZ, JSON_GZ, JSONL_GZ]:
        raise NotImplementedError("Compression is not supported yet")

    if file_format in [CSV, CSV_GZ]:
        df = pl.read_csv(path)
    elif file_format in [JSON, JSON_GZ]:
        df = pl.read_json(path)
    elif file_format in [JSONL, JSONL_GZ]:
        df = pl.read_ndjson(path)
    elif file_format in [PARQUET]:
        df = pl.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    return df


def write_dataframe(df: pl.DataFrame, path: str, file_format: Optional[str] = None):
    path = realpath(path)
    if not file_format:
        file_format = get_file_format_from_path(path)

    if file_format in [CSV_GZ, JSON_GZ, JSONL_GZ]:
        raise NotImplementedError("Compression is not supported yet")

    logger.debug('Writing %d x %d dataframe to %s (columns: %s)', len(df), len(df.columns), path, tuple(df.columns))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if file_format in [CSV, CSV_GZ]:
        df.write_csv(path)

    elif file_format in [JSON, JSON_GZ]:
        df.write_json(path, row_oriented=True)
    elif file_format in [JSONL, JSONL_GZ]:
        df.write_ndjson(path)
    elif file_format in [PARQUET]:
        df.write_parquet(path)
    else:
        raise ValueError(f"Unsupported output format: {file_format}")
    
    logger.debug('Wrote dataframe to %s', path)


def query_dataframe_with_sql(df: pl.DataFrame, table_name: str, sql_query: str):
    sql = pl.SQLContext()
    sql.register(table_name, df)
    df = sql.execute(sql_query).collect()
    return df


def query_dataframes_with_sql(dfs: Dict[str, pl.DataFrame], sql_query: str):
    sql = pl.SQLContext()
    sql.register_many(dfs)
    df = sql.execute(sql_query).collect()
    return df


def convert_files_in_dir(
        input_dir: str,
        output_dir: Optional[str],
        input_format: Optional[str] = None,
        output_format: str = DEFAULT_FILE_FORMAT,
        overwrite: bool = False):
    
    output_format = output_format or DEFAULT_FILE_FORMAT
    output_dir = output_dir or input_dir

    start_time = datetime.datetime.now()

    input_paths = iter_paths(input_dir, include_dirs=False, recursive=False)
    if input_format:
        input_paths = filter(lambda p: get_file_format_from_path(p) == input_format, input_paths)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for input_path in input_paths:
            input_format = get_file_format_from_path(input_path)
            if input_format == output_format:
                continue

            filename = os.path.basename(input_path).replace(f'.{input_format}', f'.{output_format}')
            output_path = os.path.join(output_dir, filename)
            if os.path.exists(output_path):
                logger.debug(f"Skipping {input_path} because {output_path} already exists")
                continue

            future = executor.submit(convert_file, input_path, output_path, overwrite=overwrite)
            futures.append(future)
        
        if futures:
            total_files = len(futures)
            logger.info('Converting %d files in %s into %s format and writing to %s', total_files, input_dir, output_format, output_dir)  
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                future.result()
                logger.debug('Converted %d/%d files', i + 1, total_files)
            
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info('Converted %d files in %.2f seconds', total_files, duration)


def convert_file(input_file: str, output_file: str, overwrite: bool = False):
    if os.path.exists(output_file) and not overwrite:
        logger.debug(f"Skipping {input_file} because {output_file} already exists")
        return

    df = read_dataframe(input_file)
    write_dataframe(df, output_file)


def sort_dataframe_file(
        path: str,
        select_columns: Optional[Union[str, Iterable[str]]] = None,
        sort_by: Optional[Union[str, Iterable[str]]] = None,
        sort_rows_descending: Optional[bool] = None,
        file_format: Optional[str] = None):
    
    if not any((select_columns, sort_by, sort_rows_descending)):
        raise ValueError("At least one of select_columns, sort_by, or sort_rows_descending must be specified")
    
    df = read_dataframe(path=path, file_format=file_format)
    df = sort_dataframe(
        df=df, 
        select_columns=select_columns, 
        sort_by=sort_by, 
        sort_rows_descending=sort_rows_descending,
    )
    write_dataframe(df=df, path=path, file_format=file_format)


def sort_dataframe(
        df: pl.DataFrame,
        select_columns: Optional[Union[str, Iterable[str]]] = None,
        sort_by: Optional[Union[str, Iterable[str]]] = None,
        sort_rows_descending: Optional[bool] = False):
    
    if sort_by is not None:
        df = sort_dataframe_rows(df, by=sort_by, descending=sort_rows_descending)

    if select_columns is not None:
        df = sort_dataframe_columns(df, by=select_columns)
    
    return df


def sort_dataframe_rows(
        df: pl.DataFrame, 
        by: Union[str, Iterable[str]], 
        descending: Optional[bool] = False) -> pl.DataFrame:

    descending = descending or False
    if not by:
        return df

    by = [by] if isinstance(by, str) else list(by)
    df = df.sort(by=by, descending=descending)
    return df


def sort_dataframe_columns(df: pl.DataFrame, by: Union[str, Iterable[str]]) -> pl.DataFrame:
    descending = descending or False
    if not by:
        return df
    
    by = [by] if isinstance(by, str) else list(by)
    df = df.select(by)
    return df


def rejig_dataframe_precision(df: pl.DataFrame, n: int, cols: Optional[Iterable[str]] = None) -> pl.DataFrame:
    """
    Rejig the floating point precision of the floating point columns in a dataframe.
    """
    cols = cols or df.select([pl.col(pl.FLOAT_DTYPES)]).columns
    df = df.with_columns(
        [pl.col(c).round(n) for c in cols]
    )
    return df


def rejig_fp_precision_of_file(
        path: str,
        n: int,
        cols: Optional[Iterable[str]] = None):
    
    df = read_dataframe(path)
    df = rejig_dataframe_precision(df, n=n, cols=cols)
    write_dataframe(df, path)


def rejig_fp_precision_of_files(
        input_files: Iterable[str],
        n: int,
        cols: Optional[Iterable[str]] = None):
    
    start_time = datetime.datetime.now()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for input_file in input_files:
            future = executor.submit(rejig_fp_precision_of_file, path=input_file, n=n, cols=cols)
            futures.append(future)
        
        total_files = len(futures)
        logger.info('Rejigging precision of %d files', total_files)  
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            future.result()
            logger.debug('Rejigged precision of %d/%d files', i + 1, total_files)
        
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info('Rejigged precision of %d files in %.2f seconds', total_files, duration)


def rejig_fp_precision_of_files_in_dir(
        input_dir: str, 
        n: int, 
        cols: Optional[Iterable[str]] = None):
    
    paths = iter_paths(input_dir, recursive=True)
    rejig_fp_precision_of_files(input_files=paths, n=n, cols=cols)


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