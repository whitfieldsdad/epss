#!/bin/bash

source util.sh

URL=https://epss.cyentia.com/epss_scores-current.csv.gz

gz_csv_file=${1:-$(basename ${URL})}
csv_file="${gz_csv_file%.*}"
json_file="${csv_file%.*}.json"

download_latest_epss_scores "${URL}" "${gz_csv_file}"
decompress_file "${gz_csv_file}"
strip_csv_file_comment_header "${csv_file}"
sort_csv_file "${csv_file}" "epss"
#convert_csv_to_jsonl "${csv_file}" "${json_file}"
#convert_jsonl_to_json "${json_file}" "${json_file}"
