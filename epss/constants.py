import datetime
from typing import Union

# EPSS scores were first published on 2022-07-15
MIN_DATE = "2022-07-15"

TIME = Union[datetime.date, datetime.datetime, str, int, float]

CSV = 'csv'
CSV_GZ = 'csv.gz'
JSON = 'json'
JSONL = 'jsonl'
JSON_GZ = 'json.gz'
JSONL_GZ = 'jsonl.gz'
PARQUET = 'parquet'

FILE_FORMATS = [CSV, CSV_GZ, JSON, JSONL, JSON_GZ, JSONL_GZ, PARQUET]
DEFAULT_FILE_FORMAT = PARQUET

OVERWRITE = False

CVE = 'cve'
DATE = 'date'
PARTITIONING_KEYS = {CVE, DATE}
DEFAULT_PARTITIONING_KEY = DATE