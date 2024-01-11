#!/bin/bash

source util.sh

URL=https://epss.cyentia.com/epss_scores-current.csv.gz

gz_csv_file=${1:-$(basename ${URL})}
csv_file="${gz_csv_file%.*}"

download_latest_epss_scores "${URL}" "${gz_csv_file}"
decompress_file "${gz_csv_file}"
strip_csv_file_comment_header "${csv_file}"
sort_csv_file "${csv_file}" "epss"
