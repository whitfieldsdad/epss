import json
import os
import click
import logging
from typing import Iterable, Optional
from epss.constants import *
from epss import epss, util
from epss.json_encoder import JSONEncoder

logger = logging.getLogger(__name__)


@click.group()
@click.option('--debug', is_flag=True, help='Enable debug logging')
def main(debug: bool):
    """
    Exploit Prediction Scoring System (EPSS) client
    """
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


@main.command('download')
@click.option('--output-dir', '-o', default=os.getcwd(), show_default=True, help='Output directory')
@click.option('--date', '-d', help='Date to download scores for')
@click.option('--min-date', '-a', help='Minimum date to download scores for')
@click.option('--max-date', '-b', help='Maximum date to download scores for')
@click.option('--all', 'download_all', is_flag=True, help='Download all scores')
@click.option('--cve-id', 'cve_ids', multiple=True, help='CVE IDs')
@click.option('--output-format', default=DEFAULT_FILE_FORMAT, type=click.Choice(FILE_FORMATS), help='Output format')
@click.option('--overwrite', is_flag=True, help='Overwrite existing files')
def download_scores_command(
    output_dir: str, 
    date: Optional[str], 
    min_date: Optional[str], 
    max_date: Optional[str], 
    download_all: bool,
    cve_ids: Optional[Iterable[str]], 
    output_format: Optional[str], 
    overwrite: bool):
    """
    Download EPSS scores to a directory.
    """
    if download_all:
        if min_date or max_date:
            raise ValueError("Cannot specify --all with --min-date or --max-date")
        elif date:
            raise ValueError("Cannot specify --all with --date")

        min_date = MIN_DATE
        max_date = epss.get_max_date()
    
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
        min_date = max_date = epss.get_max_date()

    epss.download_scores_over_time(
        cve_ids=cve_ids, 
        min_date=min_date, 
        max_date=max_date, 
        output_dir=output_dir, 
        file_format=output_format, 
        overwrite=overwrite,
    )


@main.command('diff')
@click.argument('a')
@click.argument('b')
@click.option('--output-file', '-o', help='Output file')
@click.option('--output-format', '-f', default=DEFAULT_FILE_FORMAT, type=click.Choice(FILE_FORMATS), help='Output file format')
def diff_command(a: str, b: str, output_file: Optional[str], output_format: Optional[str]):
    """
    Diff two sets of EPSS scores.
    """
    a = epss.read_dataframe(a)
    b = epss.read_dataframe(b)
    df = epss.diff(a, b)

    if output_file:
        util.write_polars_dataframe(df, path=output_file, file_format=output_format)
    else:
        df['date'] = df['date'].astype(str)
        for _, row in df.iterrows():
            print(json.dumps(row.to_dict(), cls=JSONEncoder))


# TODO: dynamically reconstruct snapshots when performing rolling diffs or use a static snapshot
@main.command('rolling-diff')
@click.option('--input-dir', '-i', required=True, help='Input directory')
@click.option('--output-file', '-o', required=False, help='Output file/directory')
@click.option('--output-format', '-f', default=DEFAULT_FILE_FORMAT, type=click.Choice(FILE_FORMATS), help='Output file format')
@click.option('--partition-by', '-p', type=click.Choice(['date', 'cve']), help='Partitioning scheme')
def rolling_diff_command(input_dir: str, output_file: str, output_format: Optional[str], partition_by: str):
    """
    Diff sets of EPSS scores.
    """
    paths = util.iter_paths(input_dir)
    for (_, b, df) in epss.rolling_diff_from_files(paths):        
        df['date'] = df['date'].astype(str)

        if output_file:
            path = os.path.join(output_file, f'{b.isoformat()}.{output_format}')
            util.write_polars_dataframe(df, path=path)
        else:
            raise NotImplementedError()


@main.command('merge')
@click.option('--input-dir', '-i', required=True, help='Input directory')
@click.option('--output-file', '-o', required=True, help='Output file')
@click.option('--file-format', '-f', default=DEFAULT_FILE_FORMAT, type=click.Choice(FILE_FORMATS), help='Output format')
def merge_dataframes_command(input_dir: str, output_file: str, file_format: str):
    """
    Merge a directory of EPSS scores into a single file.
    """
    paths = util.iter_paths(input_dir)
    dfs = map(util.read_polars_dataframe, paths)
    df = util.merge_dataframes(dfs)
    util.write_polars_dataframe(df=df, path=output_file, file_format=file_format)


@main.command('convert')
@click.option('--input-file', '-i', required=True, help='Input file')
@click.option('--output-file', '-o', required=True, help='Output file')
def convert_dataframe_command(input_file: str, output_file: str):
    """
    Convert matrix files between formats.
    """
    df = util.read_polars_dataframe(input_file)
    util.write_polars_dataframe(df=df, path=output_file)


@main.command('date-range')
@click.option('--input-file', '-i')
def date_range_command(input_file: str):
    """
    Print the date range of a file.
    """
    df = util.read_polars_dataframe(input_file)
    min_date = df['date'].min()
    max_date = df['date'].max()
    print(f"{min_date} - {max_date}")


@main.command('dates')
@click.option('--input-file', '-i')
def dates_command(input_file: str):
    """
    Print the dates found in a file.
    """
    df = util.read_polars_dataframe(input_file)
    dates = df['date'].unique()
    for date in sorted(dates):
        print(date)


if __name__ == '__main__':
    main()
