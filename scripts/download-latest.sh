#!/bin/bash

# This script:
#
# - Downloads the latest EPSS scores from the Cyentia Institute's website in GZIP compressed CSV format
# - Decompresses the CSV file
# - Strips the header
# - Sorts the CSV file based on the value of the "epss" column in descending order
# - Converts the CSV file to JSON and JSONL files using the Python 3 standard library

# Fail fast.
set -euo pipefail

URL=https://epss.cyentia.com/epss_scores-current.csv.gz

# The output file should be $1 or the the basename of the URL
GZ_CSV_FILE=${1:-$(basename ${URL})}
CSV_FILE="${GZ_CSV_FILE%.*}"
JSONL_FILE="${CSV_FILE%.*}.jsonl"
JSON_FILE="${CSV_FILE%.*}.json"

# Download and decompress the latest CSV file.
wget "${URL}" -O "${GZ_CSV_FILE}" --quiet
gunzip ${GZ_CSV_FILE} --force --quiet --keep

# Strip the header from the CSV file.
sed -i '' '1d' ${CSV_FILE}

# Sort the CSV file based on the value of the "epss" column in descending order.
TEMP_FILE="${CSV_FILE%.*}-temp.csv"
cat $CSV_FILE | awk 'NR == 1; NR > 1 {print $0 | "sort -nr -t, -k2"}' > $TEMP_FILE
mv $TEMP_FILE $CSV_FILE

# Convert the CSV file to JSONL.
cat ${CSV_FILE} | python3 -c 'import csv, json, sys; print("\n".join([json.dumps(dict(line)) for line in csv.DictReader(sys.stdin)]))' > ${JSONL_FILE}

# Convert the JSONL file to JSON.
cat ${JSONL_FILE} | python3 -c 'import json, sys; print(json.dumps([json.loads(line) for line in sys.stdin]))' > ${JSON_FILE}
