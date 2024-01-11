#!/bin/bash

source util.sh

min_date=$(get_min_date)
max_date=$(get_max_date)
urls=$(get_download_urls ${min_date} ${max_date})

output_dir=${1:-data}

mkdir -vp ${output_dir}
download_files "${urls}" "${output_dir}"