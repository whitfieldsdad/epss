import sys
from typing import Optional, Tuple
from epss.constants import *
from epss.client import PolarsClient as Client, Query
from epss.json_encoder import JSONEncoder
from epss import util
import polars as pl
import logging
import click
import json

logger = logging.getLogger(__name__)

# No limit on output length
cfg = pl.Config()
cfg.set_tbl_rows(-1)

DEFAULT_TABLE_NAME = 'df'

TABLE = 'table'
OUTPUT_FORMATS = FILE_FORMATS + [TABLE]

DEFAULT_FILE_OUTPUT_FORMAT = PARQUET
DEFAULT_CONSOLE_OUTPUT_FORMAT = TABLE


@click.group()
@click.option('--file-format', default=DEFAULT_FILE_FORMAT, type=click.Choice(FILE_FORMATS), show_default=True, help='File format')
@click.option('--include-v1-scores/--exclude-v1-scores', is_flag=True, help='Include v1 scores')
@click.option('--include-v2-scores/--exclude-v2-scores', is_flag=True, help='Include v2 scores')
@click.option('--include-v3-scores/--exclude-v3-scores', default=True, help='Include v3 scores')
@click.option('--include-all-scores/--exclude-all-scores', '-A', default=False, help='Include scores produced by all model versions')
@click.option('--verify-tls/--no-verify-tls', default=True, help='Verify TLS certificates when downloading scores')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def main(
    ctx: click.Context, 
    file_format: str,
    include_v1_scores: bool,
    include_v2_scores: bool,
    include_v3_scores: bool,
    include_all_scores: bool,
    verify_tls: bool,
    verbose: bool):
    """
    Exploit Prediction Scoring System (EPSS)
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(name)s %(message)s')

    if include_all_scores:
        include_v1_scores = True
        include_v2_scores = True
        include_v3_scores = True

    ctx.obj = {
        'client': Client(
            file_format=file_format,
            include_v1_scores=include_v1_scores,
            include_v2_scores=include_v2_scores,
            include_v3_scores=include_v3_scores,
            verify_tls=verify_tls,
        ),
    }


@main.command('scores')
@click.option('--workdir', '-w', required=True, help='Work directory')
@click.option('--min-date', '-a', show_default=True, help='Minimum date')
@click.option('--date', '-d', help='Date')
@click.option('--max-date', '-b', help='Maximum date')
@click.option('--output-file', '-o', help='Output file')
@click.option('--output-format', '-f', type=click.Choice(OUTPUT_FORMATS), help='Output format')
@click.option('--download', is_flag=True, help="Don't write to an output file or the console, just download the data")
@click.pass_context
def get_scores_cli(
    ctx: click.Context, 
    workdir: str,
    min_date: Optional[str],
    date: Optional[str],
    max_date: Optional[str],
    output_file: Optional[str],
    output_format: Optional[str],
    download: bool):
    """
    Get scores
    """
    if date:
        min_date = date
        max_date = date

    client: Client = ctx.obj['client']
    if download:
        client.download_scores(
            workdir=workdir,
            min_date=min_date,
            max_date=max_date,
        )
    else:
        df = client.get_scores(
            workdir=workdir,
            min_date=min_date,
            max_date=max_date,
        )
        write_output(df, output_file, output_format)


@main.command('urls')
@click.option('--min-date', '-a', show_default=True, help='Minimum date')
@click.option('--max-date', '-b', help='Maximum date')
@click.option('--date', '-d', help='Date')
@click.pass_context
def get_urls_cli(
    ctx: click.Context, 
    min_date: Optional[str],
    max_date: Optional[str],
    date: Optional[str]):
    """
    Get URLs
    """
    client: Client = ctx.obj['client']

    if date:
        min_date = date
        max_date = date

    urls = client.iter_urls(
        min_date=min_date,
        max_date=max_date,
    )
    for url in urls:
        print(url)


@main.command('date-range')
@click.option('--min-date', '-a', help='Minimum date')
@click.option('--max-date', '-b', help='Maximum date')
@click.pass_context
def get_date_range_cli(
    ctx: click.Context, 
    min_date: Optional[str],
    max_date: Optional[str]):
    """
    Preview date ranges
    """
    client: Client = ctx.obj['client']
    min_date, max_date = client.get_date_range(
        min_date=min_date,
        max_date=max_date,
    )
    print(json.dumps({
        'min_date': min_date.isoformat(),
        'max_date': max_date.isoformat(),
    }, cls=JSONEncoder))


def write_output(df: pl.DataFrame, output_file: Optional[str], output_format: Optional[str]):
    if output_file:
        output_format = output_format or DEFAULT_FILE_OUTPUT_FORMAT
        util.write_dataframe(df, output_file)
    else:
        output_format = output_format or DEFAULT_CONSOLE_OUTPUT_FORMAT
        if output_format == TABLE:
            print(df)
        elif output_format == JSON:
            print(json.dumps(df.to_dicts(), cls=JSONEncoder))
        elif output_format == JSONL:
            for d in df.to_dicts():
                print(json.dumps(d, cls=JSONEncoder))
        elif output_format == CSV:
            print(df.write_csv()) 
        else:
            raise ValueError(f"Invalid output format: {output_format}")


if __name__ == '__main__':
    main()
