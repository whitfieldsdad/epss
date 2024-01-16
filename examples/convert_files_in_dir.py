from typing import Optional
from epss import util
from epss.constants import FILE_FORMATS

import logging

logger = logging.getLogger(__name__)


def main(
        input_dir: str, 
        input_format: Optional[str], 
        output_dir: str, 
        output_format: Optional[str], 
        overwrite: bool):

    util.convert_files_in_dir(
        input_dir=input_dir,
        input_format=input_format,
        output_dir=output_dir, 
        output_format=output_format,
        overwrite=overwrite
    )


if __name__ == "__main__":
    import argparse
    
    def cli():
        parser = argparse.ArgumentParser()
        parser.add_argument("--input-dir", '-i', type=str, required=True)
        parser.add_argument("--output-dir", '-o', type=str, required=False)
        parser.add_argument("--output-format", type=str, choices=FILE_FORMATS, required=True)
        parser.add_argument('--overwrite', '-f', action='store_true')
        parser.add_argument("--verbose", '-v', action='store_true')

        kwargs = vars(parser.parse_args()) 
        
        level = logging.DEBUG if kwargs.pop('verbose') else logging.INFO
        logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        main(**kwargs)
    
    cli()
