from epss.epss import Client

import logging

logger = logging.getLogger(__name__)


def main(workdir: str, delete_downloads: bool):
    client = Client(workdir=workdir)
    client.clear(delete_downloads=delete_downloads)


if __name__ == "__main__":
    import argparse
    
    def cli():
        parser = argparse.ArgumentParser()
        parser.add_argument("workdir", type=str)
        parser.add_argument("--delete-downloads", action='store_true')
        parser.add_argument("--verbose", '-v', action='store_true')
        kwargs = vars(parser.parse_args()) 
        
        level = logging.DEBUG if kwargs.pop('verbose') else logging.INFO
        logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        main(**kwargs)
    
    cli()
