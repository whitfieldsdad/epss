#!/bin/bash

# Function to download a file from a given URL.
download_file() {
    local url=$1
    local path=$2

    echo "Downloading ${url} to ${path}"

    wget "${url}" -O "${path}" --quiet
}

# Function to download a list of files to a directory.
download_files() {
    local urls=$1
    local directory=$2

    # Count files
    local total=$(echo ${urls[@]} | wc -w | tr -d ' ')

    # Strip trailing slash from directory path
    directory=${directory%/}

    # Check if aria2c is installed
    if aria2c_installed; then
        echo "aria2c installed - downloading ${total} files in parallel"
        
        # Write list of URLs to a temporary file with one line per URL
        local url_file=.urls.txt
        echo ${urls[@]} | tr ' ' '\n' > ${url_file}

        # Download files
        aria2c -i ${url_file} --dir=${directory} --continue=true --auto-file-renaming=false

        # Remove temporary file
        rm ${url_file}
    else
        echo "aria2c not installed - downloading ${total} files one-by-one"
        for url in ${urls[@]}; do
            local path=${directory}/$(basename ${url})
            download_file "${url}" "${path}"
        done
    fi
    

    
}

aria2c_installed() {
    if [[ -z $(which aria2c) ]]; then
        echo "aria2c is not installed"
        return 1
    fi
}

# Function to download the latest set of EPSS scores from the Cyentia Institute's website.
download_latest_epss_scores() {
    local url=https://epss.cyentia.com/epss_scores-current.csv.gz
    local path=${2:-$(basename ${url})}

    download_file "${URL}" "${path}"
}

get_download_urls() {
    local min_date=$1
    local max_date=$2

    if [[ -z ${min_date} ]]; then
        min_date=$(get_min_date)
    fi

    if [[ -z ${max_date} ]]; then
        max_date=$(get_max_date)
    fi

    local urls=()

    while [[ ${min_date} < ${max_date} ]]; do
        local url=$(get_download_url ${min_date})
        urls+=(${url})
        min_date=$(date -j -v+1d -f "%Y-%m-%d" ${min_date} +%Y-%m-%d)
    done

    echo ${urls[@]}
}

get_download_url() {
    local date=$1
    local url=https://epss.cyentia.com/epss_scores-${date}.csv.gz
    echo ${url}
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
