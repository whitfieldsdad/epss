import datetime
import os
from typing import Iterable, Union
import tempfile

# Cache directory
CACHE_DIR = os.path.join(tempfile.gettempdir(), '476c9b0d-79c6-4b7e-a31a-e18cec3d6444')

# EPSS scores were first published on 2022-07-15
MIN_DATE = "2022-07-15"

# Type hints
TIME = Union[datetime.date, datetime.datetime, str, int, float]
STRS = Iterable[str]

# File formats
CSV = 'csv'
JSON = 'json'
JSONL = 'jsonl'
PARQUET = 'parquet'

FILE_FORMATS = [CSV, JSON, JSONL, PARQUET]
DEFAULT_FILE_FORMAT = PARQUET

# File handling
OVERWRITE = False

# Score keys
EPSS = 'epss'
PERCENTILE = 'percentile'
CVE = 'cve'
DATE = 'date'

# Partitioning keys
PARTITIONING_KEYS = {CVE, DATE}
DEFAULT_PARTITIONING_KEY = DATE

# Sorting keys
DATE_AND_CVE = (DATE, CVE)
DATE_AND_EPSS = (DATE, EPSS)
DEFAULT_SORTING_KEY = DATE_AND_CVE

# How many points to use for floating point precision
FLOATING_POINT_PRECISION = 5
