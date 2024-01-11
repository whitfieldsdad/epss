# Bash scripts for working with EPSS scores

The following bash scripts can be used to download and merge sets of EPSS scores:

| Script | Description |
| --- | --- |
| `download-all.sh` | Download all historical sets of EPSS scores to a directory |
| `download-latest.sh` | Download the latest set of EPSS scores to a directory |
| `merge.sh` | Merge one or more sets of EPSS scores into a single CSV or CSV.GZ file |
| `get-online-date-range.sh` | Get the range of dates for which EPSS scores are available via the [Cyentia Institute's](https://cyentia.com) website |
| `get-offline-date-range.sh` | Get the range of dates for which EPSS scores are available in the local repository |

## Requirements

- bash 4.2+ (for associative arrays)

> ⚠️ macOS ships with bash 3.2.57, which is too old. You can install a newer version of bash using [Homebrew](https://brew.sh/) (`brew install bash`).

## Usage

- [Download the latest set of EPSS scores](#download-the-latest-set-of-epss-scores)
- [Download all historical sets of EPSS scores](#download-all-historical-sets-of-epss-scores)
- [Merge a directory of EPSS scores into a single file](#merge-a-directory-of-epss-scores-into-a-single-file)
- [Get the range of dates for which EPSS scores are available via the Cyentia Institute's website](#get-the-range-of-dates-for-which-epss-scores-are-available-via-the-cyentia-institutes-website)
- [Get the range of dates for which EPSS scores are available in the local repository](#get-the-range-of-dates-for-which-epss-scores-are-available-in-the-local-repository)

### Download the latest set of EPSS scores

```bash
./download-all.sh data/
```

### Download all historical sets of EPSS scores

```bash
./download-latest.sh data/
```

### Merge a directory of EPSS scores into a single file

```bash
./merge-dir.sh data/ epss-scores.csv.gz
```

### Get the range of dates for which EPSS scores are available via the [Cyentia Institute's](https://cyentia.com) website

```bash
./get-online-date-range.sh 
```

```text
2022-07-15,2024-01-11
```

### Get the range of dates for which EPSS scores are available in the local repository

```bash
./get-offline-date-range.sh 
```

```text
2022-07-15,2024-01-11
```
