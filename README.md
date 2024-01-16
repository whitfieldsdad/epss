# Exploit Prediction Scoring System (EPSS) tooling

This repository contains a lightning-fast [Python 3 module](epss) and a series of [bash scripts](scripts) that are designed to make it easy for anyone to work with the daily outputs of the [Exploit Prediction Scoring System (EPSS)](https://www.first.org/epss/).

## Background

The Exploit Prediction Scoring System (EPSS) is a probabilistic scoring system that is designed to predict the likelihood that a computer security vulnerability will be exploited somewhere in the wild within the next 30 days.

EPSS scores are calculated on a daily basis using the output of a machine learning model, and ~99.9% of scores stay the same from day-to-day.

With ~220,000 CVEs in the NVD, this means that ~220,000 data points are added each day, but only ~2,200 of those data points actually change.

## Design

- EPSS scores are downloaded from the Cyentia Institute's website in CSV.GZ format and transformed into Parquet files at download time.
- EPSS scores are reduced to a changelog containing only metrics that have changed since the last measurement.
- The changelog is partitioned by date and CVE ID.

## Features

- Download and query date partitioned sets of EPSS scores
- Search for vulnerabilities using dimensions sourced from the NIST NVD, CISA KEV Catalog, MITRE ATT&CK, MITRE CAPEC, MITRE CWE, and/or MITRE CPE (e.g. to identify changes in the perceived exploitability of vulnerabilities affecting Microsoft Exchange that are known to have been used in a ransomware attack in the wild)
- Calculate changelogs between any two or more sets of EPSS scores using [Polars](https://pola.rs/)
- Calculate the range of EPSS scores for any CVE ID over any time period (e.g. for the last 7 days, 30 days, 90 days, 180 days, since metrics were first published, etc.)
- Optionally drop any EPSS scores that haven't changed since the last measurement (~99% of all scores)
- Optionally drop any EPSS scores that haven't changed by more than (n) percent since the last measurement
- Optionally reduce the precision of EPSS scores to reduce the size of the changelog (e.g. from 5 decimal places to 2, or even 0 if you only care about changes of at least ~1%)
- Translate metrics into [Vega-Lite](https://vega.github.io/vega-lite/) format using [Altair](https://altair-viz.github.io/) to allow for easy, plug-and-play visualization with any [Vega-Lite compatible tool](https://vega.github.io/vega-lite/ecosystem.html) (e.g. [Voyager](https://vega.github.io/voyager/), [Vega-Editor](https://vega.github.io/editor/#/))
