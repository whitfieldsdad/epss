from epss.client import PolarsClient

import polars as pl
import logging
import tempfile
import os

cfg = pl.Config()
cfg.set_tbl_rows(-1)    # Unlimited output length

logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(name)s %(message)s')

WORKDIR = os.path.join(tempfile.gettempdir(), 'epss')

client = PolarsClient(
    include_v1_scores=False,
    include_v2_scores=False,
    include_v3_scores=True,
)
df = client.get_scores_by_date(
    workdir=WORKDIR,
    date='2024-01-01',
)
print(df)
