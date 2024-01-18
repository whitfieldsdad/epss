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


@main.command('diffs')
@click.option('--cve-id', '-c', 'cve_ids', multiple=True, help='CVE IDs')
@click.option('--min-date', '-a', default=MIN_DATE, show_default=True, help='Minimum date')
@click.option('--max-date', '-b', default=None, help='Maximum date')
@click.option('--date', '-d', default=None, help='Date')
@click.option('--days-ago', '-n', type=int, default=None, help='Number of days ago')
@click.option('--output-file', '-o', default=None, help='Output file/directory')
@click.option('--sql-query', '-s', default=None, help='SQL query')
@click.option('--sql-table-name', '-t', default=DEFAULT_TABLE_NAME, show_default=True, help='SQL table name')
@click.pass_context
def get_diffs_cli(
    ctx: click.Context, 
    cve_ids: Optional[Iterable[str]], 
    min_date: Optional[str],
    max_date: Optional[str],
    date: Optional[str],
    days_ago: Optional[int],
    output_file: Optional[str],
    sql_query: Optional[str],
    sql_table_name: Optional[str]):
    """
    Get a rolling diff of scores over time between two dates.
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
    if cve_ids:
        query = Query(cve_ids=cve_ids)

    df = client.get_historical_diff_dataframe(
        query=query,
        min_date=min_date, 
        max_date=max_date,
    )

    if sql_query:
        sql_table_name = sql_table_name or DEFAULT_TABLE_NAME
        df = util.query_dataframes_with_sql({sql_table_name: df}, sql_query=sql_query)

    if output_file:
        util.write_dataframe(df, output_file)
    else:
        print(df)
    

@main.command('diff')
@click.argument('cve-ids', nargs=-1, required=False, type=str)
@click.option('--min-date', '-a', default=MIN_DATE, show_default=True, help='Minimum date')
@click.option('--max-date', '-b', default=None, help='Maximum date')
@click.option('--date', '-d', default=None, help='Date')
@click.option('--days-ago', '-n', type=int, default=None, help='Number of days ago')
@click.option('--sql-query', '-s', default=None, help='SQL query')
@click.option('--sql-table-name', '-t', default=DEFAULT_TABLE_NAME, show_default=True, help='SQL table name')
@click.option('--output-file', '-o', default=None, help='Output file/directory')
@click.pass_context
def get_diff_cli(
    ctx: click.Context, 
    cve_ids: Optional[Iterable[str]], 
    min_date: Optional[str],
    max_date: Optional[str],
    date: Optional[str],
    days_ago: Optional[int],
    sql_query: Optional[str],
    sql_table_name: Optional[str],
    output_file: Optional[str]):
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
    logger.info("Calculating diff from %s to %s", min_date.isoformat(), max_date.isoformat())

    query = None
    if cve_ids:
        query = Query(cve_ids=cve_ids)

    df = client.get_score_diff_dataframe(
        query=query,
        first_date=min_date,
        second_date=max_date,
    )

    if sql_query:
        sql_table_name = sql_table_name or DEFAULT_TABLE_NAME
        df = util.query_dataframes_with_sql({sql_table_name: df}, sql_query=sql_query)

    if output_file:
        util.write_dataframe(df, output_file)
    else:
        print(df)


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


if __name__ == '__main__':
    main()
