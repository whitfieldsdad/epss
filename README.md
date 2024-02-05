# Exploit Prediction Scoring System (EPSS) tooling

This repository contains a lightning-fast [Python 3 module](epss) and a series of [bash scripts](scripts) that are designed to make it easy for anyone to work with the daily outputs of the [Exploit Prediction Scoring System (EPSS)](https://www.first.org/epss/).

## Features

- Idempotently download daily sets of EPSS scores<sub>1</sub> in JSON, JSONL, CSV, or [Apache Parquet](https://parquet.apache.org/)<sub>2</sub> format
- Explore EPSS scores using [Polars](https://pola.rs/), a lightning-fast dataframe library written in Rust
- Optionally drop unchanged scores<sub>3</sub>
- Optionally disable TLS certificate validation when downloading scores (i.e. to support environments where TLS MitM is being performed)
- [Easily](examples/get-scores-as-polars-dataframe.py) [switch](examples/get-changed-scores-as-polars-dataframe.py) between different versions<sub>4</sub> of the [EPSS model](https://www.first.org/epss/model)

<sub>1. By default, EPSS scores will be downloaded from 2023-03-07 onward, as this is the date when the outputs of EPSS v3 (v2023.03.01) were first published.</sub>

<sub>2. Apache Parquet is the default file format.</sub>

<sub>3. The [Cyentia Institute](https://www.cyentia.com/research/) [publishes](https://www.first.org/epss/data_stats) sets of EPSS scores partitioned by date on a daily basis in GZIP compressed CSV format.</sub>

<sub>4. EPSS has undergone 3 major revisions: [EPSS v1](https://arxiv.org/abs/1908.04856), EPSS v2 (v2022.01.01), and [EPSS v3 (v2023.03.01)](https://arxiv.org/abs/2302.14172) where the first, second, and third revisions all contain major improvements.</sub>

## Background

The Exploit Prediction Scoring System (EPSS) is a probabilistic [model](https://www.first.org/epss/model) that is designed to predict the likelihood of a given computer security vulnerability being exploited somewhere in the wild within the next 30 days.

The first version of the EPSS model was released in 2021, and it has since undergone two major revisions.

The first version of the EPSS model used logistic regression, but subsequent models have used [gradient-boosted decision trees](https://en.wikipedia.org/wiki/Gradient_boosting) ([XGBoost](https://en.wikipedia.org/wiki/XGBoost)) to make predictions.

For additional information on EPSS and its applications, please consult the following resources:

- [Exploit Prediction Scoring System (EPSS)](https://arxiv.org/abs/1908.04856)
- [Enhancing Vulnerability Prioritization: Data-Driven Exploit Predictions with Community-Driven Insights](https://arxiv.org/abs/2302.14172)

Additional resources:

- [Daily analysis of EPSS scores](https://www.first.org/epss/data_stats)
- [The Exploit Prediction Scoring System (EPSS) Explained](https://www.splunk.com/en_us/blog/learn/epss-exploit-prediction-scoring-system.html#:~:text=In%20short%2C%20EPSS%20allows%20us,vulnerability%20might%20be%20if%20exploited.)
- [F5 Labs Joins the Exploit Prediction Scoring System as a Data Partner](https://www.f5.com/labs/articles/cisotociso/f5-labs-joins-the-exploit-prediction-scoring-system-as-a-data-partner)

## Usage

### Building

This package is not currently available on PyPi, but can be easily added to your project in one of two ways:

- Using `poetry`<sub>1</sub>:

```
poetry add git+https://github.com/whitfieldsdad/epss.git
```

By branch:

```
poetry add git+https://github.com/whitfieldsdad/epss.git#main
```

By tag:

```
poetry add git+https://github.com/whitfieldsdad/epss.git#v3.0.0
```


- Using `requirements.txt`:

By tag:

```
git+https://github.com/whitfieldsdad/epss@releases/tag/v3.0.0
```

By branch:

```
git+git+https://github.com/owner/repo@main
```

<sub>1. Using Poetry for dependency management and adding this project as a dependency of your project without explicitly specifying a branch or tag is recommended.</sub>

### Command line interface

#### Listing scores published between two dates

To list<sub>1</sub> all scores published since 2024 without dropping unchanged scores<sub>2</sub>:

```bash
poetry run epss scores -a 2024-01-01 --no-drop-unchanged | head
```

```text
shape: (7_992_196, 4)
┌──────────────────┬─────────┬────────────┬────────────┐
│ cve              ┆ epss    ┆ percentile ┆ date       │
│ ---              ┆ ---     ┆ ---        ┆ ---        │
│ str              ┆ f64     ┆ f64        ┆ date       │
╞══════════════════╪═════════╪════════════╪════════════╡
│ CVE-2019-2725    ┆ 0.97572 ┆ 1.0        ┆ 2024-01-01 │
│ CVE-2019-1653    ┆ 0.97567 ┆ 1.0        ┆ 2024-01-01 │
│ CVE-2015-7297    ┆ 0.97564 ┆ 0.99999    ┆ 2024-01-01 │
│ CVE-2014-6271    ┆ 0.97564 ┆ 0.99999    ┆ 2024-01-01 
...
```

```bash
poetry run epss scores -a 2024-01-01 --drop-unchanged | head
```

```text
shape: (33_592, 4)
┌──────────────────┬─────────┬────────────┬────────────┐
│ cve              ┆ epss    ┆ percentile ┆ date       │
│ ---              ┆ ---     ┆ ---        ┆ ---        │
│ str              ┆ f64     ┆ f64        ┆ date       │
╞══════════════════╪═════════╪════════════╪════════════╡
│ CVE-2019-1653    ┆ 0.97555 ┆ 0.99998    ┆ 2024-01-03 │
│ CVE-2020-14750   ┆ 0.97544 ┆ 0.99995    ┆ 2024-01-03 │
│ CVE-2013-2423    ┆ 0.97512 ┆ 0.99983    ┆ 2024-01-03 │
│ CVE-2019-19781   ┆ 0.97485 ┆ 0.99967    ┆ 2024-01-03 │
...
```

The `--output-format` argument can be used to change the output format.

For example, to list scores in CSV format:

```bash
poetry run epss scores -a 2024-01-01 --drop-unchanged --output-format=csv | head
```

```csv
cve,epss,percentile,date
CVE-2019-1653,0.97555,0.99998,2024-01-03
CVE-2020-14750,0.97544,0.99995,2024-01-03
CVE-2013-2423,0.97512,0.99983,2024-01-03
CVE-2019-19781,0.97485,0.99967,2024-01-03
CVE-2019-1652,0.9747,0.99959,2024-01-03
CVE-2013-1559,0.9728,0.99833,2024-01-03
CVE-2019-3398,0.9722,0.99798,2024-01-03
CVE-2019-1458,0.97194,0.99782,2024-01-03
CVE-2020-7209,0.9719,0.99778,2024-01-03
...
```

To save the output to a CSV file, you could use shell redirection, or the `--output-file` flag:

```bash
poetry run epss scores -a 2024-01-01 --drop-unchanged --output-format=csv --output-file 2024-01-01.csv
```

```bash
du -sh 2024-01-01.csv
1.3M    2024-01-01.csv
```

Or, in JSONL format:

```bash
poetry run epss scores -a 2024-01-01 --drop-unchanged --output-format=jsonl | head | jq -c
```

```json
{"cve":"CVE-2019-1653","epss":0.97555,"percentile":0.99998,"date":"2024-01-03"}
{"cve":"CVE-2020-14750","epss":0.97544,"percentile":0.99995,"date":"2024-01-03"}
{"cve":"CVE-2013-2423","epss":0.97512,"percentile":0.99983,"date":"2024-01-03"}
{"cve":"CVE-2019-19781","epss":0.97485,"percentile":0.99967,"date":"2024-01-03"}
{"cve":"CVE-2019-1652","epss":0.9747,"percentile":0.99959,"date":"2024-01-03"}
{"cve":"CVE-2013-1559","epss":0.9728,"percentile":0.99833,"date":"2024-01-03"}
{"cve":"CVE-2019-3398","epss":0.9722,"percentile":0.99798,"date":"2024-01-03"}
{"cve":"CVE-2019-1458","epss":0.97194,"percentile":0.99782,"date":"2024-01-03"}
{"cve":"CVE-2020-7209","epss":0.9719,"percentile":0.99778,"date":"2024-01-03"}
{"cve":"CVE-2021-43798","epss":0.97105,"percentile":0.99734,"date":"2024-01-03"}
```

From here, it's easy to see when specific vulnerabilities experienced an increase or decrease in their perceived exploitability:

```bash
poetry run epss scores --drop-unchanged --output-format=jsonl | 
grep "CVE-2016-0060" | jq -c
```

```json
{"cve":"CVE-2016-0060","epss":0.07609,"percentile":0.931,"date":"2023-04-04"}
{"cve":"CVE-2016-0060","epss":0.12376,"percentile":0.94566,"date":"2023-05-13"}
{"cve":"CVE-2016-0060","epss":0.51531,"percentile":0.97065,"date":"2023-06-19"}
{"cve":"CVE-2016-0060","epss":0.66813,"percentile":0.9746,"date":"2023-07-23"}
{"cve":"CVE-2016-0060","epss":0.7155,"percentile":0.97673,"date":"2023-09-28"}
{"cve":"CVE-2016-0060","epss":0.71177,"percentile":0.97697,"date":"2023-10-31"}
{"cve":"CVE-2016-0060","epss":0.7436,"percentile":0.97832,"date":"2023-12-03"}
{"cve":"CVE-2016-0060","epss":0.76991,"percentile":0.97928,"date":"2024-01-04"}
{"cve":"CVE-2016-0060","epss":0.828,"percentile":0.98183,"date":"2024-02-05"}
```

<sub>1. When querying historical sets of EPSS scores, any scores that have not already been downloaded will be downloaded automatically to a configurable working directory<sub>3</sub>. You do not have to explicitly download EPSS scores before querying them.</sub>

<sub>2. Unchanged scores are dropped by default - this behaviour can be toggled using the `--drop-unchanged/--no-drop-unchanged` flags.</sub>

<sub>3. If a working directory is not explicitly provided, scores will be written to a folder named `476c9b0d-79c6-4b7e-a31a-e18cec3d6444/epss/scores-by-date` within the system's temporary directory (e.g. `/var/folders/ps/c0fn47n54sg08wck9_x9qncr0000gp/T/476c9b0d-79c6-4b7e-a31a-e18cec3d6444/epss/scores-by-date/`).</sub>

#### Download scores published between two dates

To download scores published between two dates without writing to the console, simply add the `--download` flag<sub>1</sub>:

```bash
poetry run epss scores -a 2024-01-01 --download
```

<sub>1. Unchanged scores will still be saved to disk regardless of the value of the `--drop-unchanged/--no-drop-unchanged` flags.</sub>

### Python

Additional examples are available in the [examples](examples) folder.

#### Load unique EPSS scores into Polars

To load EPSS scores into Polars:

```python
from epss.client import PolarsClient

import polars as pl
import tempfile
import os

cfg = pl.Config()
cfg.set_tbl_rows(-1)    # Unlimited output length

WORKDIR = os.path.join(tempfile.gettempdir(), 'epss')

client = PolarsClient(
    include_v1_scores=False,
    include_v2_scores=False,
    include_v3_scores=True,
)
df = client.get_scores(workdir=WORKDIR, drop_unchanged_scores=True)
print(df)
```

#### Generating a spreadsheet of changed EPSS scores

To generate a [spreadsheet](examples/data/epss.xlsx) containing the EPSS scores of all [CVEs](https://github.com/mandiant/red_team_tool_countermeasures/blob/master/CVEs_red_team_tools.md) known to be exploitable using [FireEye's leaked red team tools](https://www.mandiant.com/resources/blog/unauthorized-access-of-fireeye-red-team-tools):

```python
from xlsxwriter import Workbook
from epss.client import PolarsClient, Query

import tempfile
import os

WORKDIR = os.path.join(tempfile.gettempdir(), 'epss')

client = PolarsClient(
    include_v1_scores=False,
    include_v2_scores=False,
    include_v3_scores=True,
)
query = Query(
    cve_ids=[
        'CVE-2019-11510',
        'CVE-2020-1472',
        'CVE-2018-13379',
        'CVE-2018-15961',
        'CVE-2019-0604',
        'CVE-2019-0708',
        'CVE-2019-11580',
        'CVE-2019-19781',
        'CVE-2020-10189',
        'CVE-2014-1812',
        'CVE-2019-3398',
        'CVE-2020-0688',
        'CVE-2016-0167',
        'CVE-2017-11774',
        'CVE-2018-8581',
        'CVE-2019-8394',
    ]
)
df = client.get_scores(
    workdir=WORKDIR,
    query=query,
    drop_unchanged_scores=True
)

with Workbook('epss.xlsx') as wb:
    df.write_excel(
        workbook=wb,
        worksheet='FireEye red team tools'
    )
```