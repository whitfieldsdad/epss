from typing import Optional
from epss import util
from epss.constants import FILE_FORMATS

import logging

logger = logging.getLogger(__name__)


def main(input_dir: str, cols: Optional[str], n: int):
    util.rejig_fp_precision_of_files_in_dir(
        input_dir=input_dir,
        cols=cols,
        n=n,
    )


if __name__ == "__main__":
    import argparse
    
    def cli():
        parser = argparse.ArgumentParser()
        parser.add_argument("--input-dir", '-i', type=str, required=True)
        parser.add_argument('--cols', '-c', nargs='+', type=str, required=False)
        parser.add_argument('--precision', '-n', type=int, default=5)
        parser.add_argument("--verbose", '-v', action='store_true')

        kwargs = vars(parser.parse_args()) 
        
        level = logging.DEBUG if kwargs.pop('verbose') else logging.INFO
        logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        kwargs['n'] = kwargs.pop('precision')

        main(**kwargs)
    
    cli()
