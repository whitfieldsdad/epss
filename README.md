# Exploit Prediction Scoring System (EPSS) tooling

This repository contains a lightning-fast [Python 3 module](epss) and a series of [bash scripts](scripts) that are designed to make it easy for anyone to work with the daily outputs of the [Exploit Prediction Scoring System (EPSS)](https://www.first.org/epss/).

🚧👷 Under construction 🏗️🚧 

## Features

- Explore EPSS scores using [Polars](https://pola.rs/), a lightning-fast dataframe library written in Rust
- Idempotently download daily sets of EPSS scores<sub>1</sub> in JSON, JSONL, CSV, or [Parquet](https://parquet.apache.org/) format<sub>2</sub>
- Easily identify changes between two or more sets of EPSS scores
- Translate sets of EPSS scores into sparse matrices to allow for the easy identification of changes to one or more computer security vulnerabilities on a daily or per CVE ID basis.

<sub>1. By default, EPSS scores will be downloaded from 2023-03-07 onward, as this is the date when the outputs of EPSS v3 (v2023.03.01) were first published.</sub>

<sub>2. Apache Parquet is the default file format.</sub>
