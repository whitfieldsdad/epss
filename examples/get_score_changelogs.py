import sys
from typing import Iterable, Optional
from epss.epss import Client, Query
from epss import util, epss

import polars as pl
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Hide log line indicating dataframe shape, allow unlimited output rows.
pl.Config.set_tbl_hide_dataframe_shape(True) 
pl.Config.set_tbl_rows(-1)


def main(
        cve_ids: Optional[Iterable[str]],
        min_date: Optional[str],
        max_date: Optional[str],
        output_file: Optional[str],
        file_format: Optional[str],
        partitioning_key: Optional[str],
        overwrite: bool):
    
    client = Client()
    query = Query(cve_ids=cve_ids)
    df = client.get_score_changelog_dataframe(
        query=query,
        min_date=min_date,
        max_date=max_date,
        preserve_order=True,
    )    
    if not output_file:
        print(df)
    else:
        if os.path.isdir(output_file):
            if partitioning_key:
                epss.write_partitioned_dataframe_to_dir(
                    df=df,
                    output_dir=output_file,
                    partitioning_key=partitioning_key,
                    file_format=file_format,
                    overwrite=overwrite,
                )
            else:
                raise ValueError("Output file is a directory, but no partitioning key was specified")
        else:
            util.write_dataframe(df, output_file)


if __name__ == "__main__":
    import argparse

    def cli():
        args = argparse.ArgumentParser()
        args.add_argument('cve_ids', nargs='*', type=str, default=None, help='CVE IDs')
        args.add_argument('--min-date', '-a', type=str, default=None, help='Minimum date')
        args.add_argument('--max-date', '-b', type=str, default=None, help='Maximum date')
        args.add_argument("--output-file", "-o", type=str, default=None, help="Output file/directory")
        args.add_argument("--file-format", type=str, default=None, choices=['csv', 'jsonl', 'parquet'], help="Output file format")
        args.add_argument('--partitioning-key', '-g', type=str, default=None, choices=['cve', 'date'], help='Partitioning key')
        args.add_argument('--force', '-f', action='store_true', help='Force overwrite of existing files')
        args.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
        kwargs = vars(args.parse_args())
        
        verbose = kwargs.pop('verbose')
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(message)s')

        kwargs.update({
            'overwrite': kwargs.pop('force'),
        })

        main(**kwargs)

    cli()
