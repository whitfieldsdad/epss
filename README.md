# Exploit Prediction Scoring System (EPSS) tooling

This repository contains a lightning-fast [Python 3 module](epss) and a series of [bash scripts](scripts) that are designed to make it easy for anyone to work with the daily outputs of the [Exploit Prediction Scoring System (EPSS)](https://www.first.org/epss/).

## Features

- Idempotently download daily sets of EPSS scores<sub>1</sub> in JSON, JSONL, CSV, or [Apache Parquet](https://parquet.apache.org/)<sub>2</sub> format
- Explore EPSS scores as either sparse or dense matrices using [Polars](https://pola.rs/), a lightning-fast dataframe library written in Rust
- [Easily](examples/get-scores-as-polars-dataframe.py) [switch](examples/get-changed-scores-as-polars-dataframe.py) between different versions<sub>3</sub> of the [EPSS model](https://www.first.org/epss/model)

<sub>1. By default, EPSS scores will be downloaded from 2023-03-07 onward, as this is the date when the outputs of EPSS v3 (v2023.03.01) were first published.</sub>

<sub>2. Apache Parquet is the default file format.</sub>

<sub>3. EPSS has undergone 3 major revisions: [EPSS v1](https://arxiv.org/abs/1908.04856), EPSS v2 (v2022.01.01), and [EPSS v3 (v2023.03.01)](https://arxiv.org/abs/2302.14172) where the first, second, and third revisions all contain major improvements.</sub>

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

### Developers

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
