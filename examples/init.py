from typing import Optional
from epss.epss import Client

import logging

logger = logging.getLogger(__name__)


def main(workdir: str, min_date: Optional[str], max_date: Optional[str]):
    client = Client(workdir=workdir)
    client.init(min_date=min_date, max_date=max_date)


if __name__ == "__main__":
    import argparse
    
    def cli():
        parser = argparse.ArgumentParser()
        parser.add_argument("workdir", type=str)
        parser.add_argument("--min-date", '-a', type=str, default=None)
        parser.add_argument("--max-date", '-b', type=str, default=None)
        parser.add_argument("--verbose", '-v', action='store_true')
        kwargs = vars(parser.parse_args()) 
        
        level = logging.DEBUG if kwargs.pop('verbose') else logging.INFO
        logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        main(**kwargs)
    
    cli()
