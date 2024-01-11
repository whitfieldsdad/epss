#!/bin/bash

# Function to download a file from a given URL.
download_file() {
    local url=$1
    local path=$2

    echo "Downloading ${url} to ${path}"

    wget "${url}" -O "${path}" --quiet
}

# Function to download the latest set of EPSS scores from the Cyentia Institute's website.
download_latest_epss_scores() {
    local url=https://epss.cyentia.com/epss_scores-current.csv.gz
    local path=${2:-$(basename ${url})}

    download_file "${URL}" "${path}"
}

get_max_date() {
    local url=https://epss.cyentia.com/epss_scores-current.csv.gz

    # HTTP HEAD and read `Location` header
    local location=$(curl -sI ${url} | grep -i location | awk '{print $2}' | tr -d '\r')
    
    # Extract date from `Location`
    date=$(echo ${location} | grep -oE '(\d{4}-\d{2}-\d{2})')
    echo ${date}
}

get_min_date() {
    echo "2022-07-15"
}

# Function to decompress a GZIP file.
decompress_file() {
    local path=$1

    gunzip ${path} --force --quiet --keep
}

# Function to strip the header from a CSV file.
strip_csv_file_comment_header() {
    local path=$1

    if [[ $(head -n 1 ${path}) == "#"* ]]; then
        echo "Stripping header from ${path}"
        sed -i '' '1d' ${path}
    fi
}

# Function to sort a CSV file based on the specified column name in descending order.
sort_csv_file() {
    local csv_file=$1
    local column_name=$2

    local temp_file="${csv_file%.*}-temp.csv"
    
    echo "Sorting ${csv_file} by ${column_name} in descending order"
    sort -t, -k2 -nr ${csv_file} > ${temp_file}
    mv $temp_file $csv_file
}

# Function to convert a CSV file to JSONL.
convert_csv_to_jsonl() {
    local csv_file=$1
    local jsonl_file=$2

    cat ${csv_file} | python3 -c 'import csv, json, sys; print("\n".join([json.dumps(dict(line)) for line in csv.DictReader(sys.stdin)]))' > ${jsonl_file}
}

# Function to convert a JSONL file to JSON.
convert_jsonl_to_json() {
    local jsonl_file=$1
    local json_file=$2

    cat ${jsonl_file} | python3 -c 'import json, sys; print(json.dumps([json.loads(line) for line in sys.stdin]))' > ${json_file}
}
