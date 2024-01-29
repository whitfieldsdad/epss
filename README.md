# Exploit Prediction Scoring System (EPSS) tooling

This repository contains a lightning-fast [Python 3 module](epss) and a series of [bash scripts](scripts) that are designed to make it easy for anyone to work with the daily outputs of the [Exploit Prediction Scoring System (EPSS)](https://www.first.org/epss/).

🚧 Under construction 🚧 

## Design

- Daily sets of EPSS scores are downloaded from the Cyentia Institute's website in CSV.GZ format, translated into Polars dataframes in-memory, and written to disk in JSON, JSONL, CSV, or Parquet format
