import sys
from typing import Optional, Tuple
from epss.constants import *
from epss.epss import Client, Query
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
@click.option('--workdir', default=CACHE_DIR, show_default=True, help='Working directory')
@click.option('--file-format', default=DEFAULT_FILE_FORMAT, type=click.Choice(FILE_FORMATS), show_default=True, help='File format')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def main(ctx: click.Context, workdir: str, file_format: str, verbose: bool):
    """
    Exploit Prediction Scoring System (EPSS)
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(name)s %(message)s')

    ctx.obj = {
        'client': Client(
            workdir=workdir,
            file_format=file_format,
        ),
    }


@main.command('init')
@click.option('--min-date', default=MIN_DATE, show_default=True, help='Minimum date')
@click.option('--max-date', default=None, help='Maximum date')
@click.pass_context
def init(ctx: click.Context, min_date: str, max_date: str):
    """
    Initialize local cache of EPSS scores.
    """
    client: Client = ctx.obj['client']
    client.init(min_date=min_date, max_date=max_date)


@main.command('clear')
@click.pass_context
def clear(ctx: click.Context):
    """
    Clear local cache of EPSS scores.
    """
    client: Client = ctx.obj['client']
    client.clear()


@main.command('history')
@click.option('--cve-id', '-c', 'cve_ids', multiple=True, help='CVE ID')
@click.option('--cve-id-file', '-i', 'cve_id_files', multiple=True, default=None, help='CVE IDs file (one per line)')
@click.option('--min-date', '-a', default=MIN_DATE, show_default=True, help='Minimum date')
@click.option('--max-date', '-b', default=None, help='Maximum date')
@click.option('--date', '-d', default=None, help='Date')
@click.option('--days-ago', '-n', type=int, default=None, help='Number of days ago')
@click.option('--sql-query', '-s', default=None, help='SQL query')
@click.option('--sql-table-name', '-t', default=DEFAULT_TABLE_NAME, show_default=True, help='SQL table name')
@click.option('--output-file', '-o', default=None, help='Output file/directory')
@click.option('--output-format', '-f', default=None, type=click.Choice(OUTPUT_FORMATS), help='Output file format')
@click.pass_context
def get_history_cli(
    ctx: click.Context, 
    cve_ids: Optional[Iterable[str]], 
    cve_id_files: Optional[str],
    min_date: Optional[str],
    max_date: Optional[str],
    date: Optional[str],
    days_ago: Optional[int],
    sql_query: Optional[str],
    sql_table_name: Optional[str],
    output_file: Optional[str],
    output_format: Optional[str]):
    """
    Get a diff of scores on two dates.
    """
    client: Client = ctx.obj['client']

    min_date, max_date = get_date_range(
        client=client,
        min_date=min_date,
        max_date=max_date,
        date=date,
        days_ago=days_ago,
    )

    query = None
    if cve_ids or cve_id_files:
        query = Query(cve_ids=cve_ids, cve_id_files=cve_id_files)

    df = client.get_score_history_dataframe(
        query=query, 
        min_date=min_date,
        max_date=max_date,
    )
    df = df.sort(['date', 'cve'])

    if sql_query:
        sql_table_name = sql_table_name or DEFAULT_TABLE_NAME
        df = util.query_dataframes_with_sql({sql_table_name: df}, sql_query=sql_query)

    write_output(df, output_file=output_file, output_format=output_format)


@main.command('diff')
@click.option('--cve-id', '-c', 'cve_ids', multiple=True, help='CVE ID')
@click.option('--cve-id-file', '-i', 'cve_id_files', multiple=True, default=None, help='CVE IDs file (one per line)')
@click.option('--min-date', '-a', default=MIN_DATE, show_default=True, help='Minimum date')
@click.option('--max-date', '-b', default=None, help='Maximum date')
@click.option('--date', '-d', default=None, help='Date')
@click.option('--days-ago', '-n', type=int, default=None, help='Number of days ago')
@click.option('--sql-query', '-s', default=None, help='SQL query')
@click.option('--sql-table-name', '-t', default=DEFAULT_TABLE_NAME, show_default=True, help='SQL table name')
@click.option('--output-file', '-o', default=None, help='Output file/directory')
@click.option('--output-format', '-f', default=None, type=click.Choice(OUTPUT_FORMATS), help='Output file format')
@click.option('--diff/--rolling', default=True, help="Whether to diff (a, b) or to calculate a rolling diff (a, a + 1, a + 2, ... a + n)")
@click.pass_context
def get_diff_cli(
    ctx: click.Context, 
    cve_ids: Optional[Iterable[str]], 
    cve_id_files: Optional[str],
    min_date: Optional[str],
    max_date: Optional[str],
    date: Optional[str],
    days_ago: Optional[int],
    sql_query: Optional[str],
    sql_table_name: Optional[str],
    output_file: Optional[str],
    output_format: Optional[str],
    diff: bool):
    """
    Get a diff of scores on two dates.
    """
    client: Client = ctx.obj['client']

    min_date, max_date = get_date_range(
        client=client,
        min_date=min_date,
        max_date=max_date,
        date=date,
        days_ago=days_ago,
    )

    query = None
    if cve_ids or cve_id_files:
        query = Query(cve_ids=cve_ids, cve_id_files=cve_id_files)

    if diff:
        df = client.get_score_diff_dataframe(
            first_date=min_date,
            second_date=max_date,
            query=query,
        )
    else:
        df = client.get_historical_diff_dataframe(
            min_date=min_date,
            max_date=max_date,
            query=query,
        )

    df = df.sort(['date', 'cve'])

    if sql_query:
        sql_table_name = sql_table_name or DEFAULT_TABLE_NAME
        df = util.query_dataframes_with_sql({sql_table_name: df}, sql_query=sql_query)

    write_output(df, output_file=output_file, output_format=output_format)


@main.command('date-range')
@click.option('--min-date', '-a', default=MIN_DATE, show_default=True, help='Minimum date')
@click.option('--max-date', '-b', default=None, help='Maximum date')
@click.option('--date', '-d', default=None, help='Date')
@click.option('--days-ago', '-n', type=int, default=None, help='Number of days ago')
@click.pass_context
def get_date_range_cli(
    ctx: click.Context, 
    min_date: Optional[str],
    max_date: Optional[str],
    date: Optional[str],
    days_ago: Optional[int]):
    """
    Preview date ranges
    """
    client: Client = ctx.obj['client']

    min_date, max_date = get_date_range(
        client=client,
        min_date=min_date,
        max_date=max_date,
        date=date,
        days_ago=days_ago,
    )

    print(json.dumps({
        'min_date': min_date.isoformat() if min_date else None,
        'max_date': max_date.isoformat() if max_date else None,
    }, cls=JSONEncoder))


def get_date_range(
        client: Client,
        min_date: Optional[str], 
        max_date: Optional[str], 
        date: Optional[str],
        days_ago: Optional[int]) -> Tuple[datetime.date, datetime.date]:

    min_date = util.parse_date(min_date) if min_date else None
    max_date = util.parse_date(max_date) if max_date else None

    if min_date == max_date:
        raise ValueError("Minimum and maximum dates cannot be the same")

    if date:
        max_date = util.parse_date(date)
        min_date = max_date - datetime.timedelta(days=1)
    elif days_ago is not None:
        max_date = client.max_date
        days_ago = max(days_ago, 1)
        min_date = max_date - datetime.timedelta(days=days_ago)
    return min_date or client.min_date, max_date or client.max_date


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
