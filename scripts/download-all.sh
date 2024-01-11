#!/bin/bash

source util.sh

output_dir=${1:-data}

min_date=$(get_min_date)
max_date=$(get_max_date)
urls=$(get_download_urls ${min_date} ${max_date})

# Determine which files have already been downloaded
pending_urls=()
for url in ${urls[@]}; do
    path=${output_dir}/$(basename ${url})
    if [[ ! -f ${path} ]]; then
        pending_urls+=(${url})
    fi
done

# Download the files that haven't been downloaded yet
if [[ ${#pending_urls[@]} -eq 0 ]]; then
    echo "All files have already been downloaded"
    exit 0
fi

mkdir -vp ${output_dir}
download_files "${pending_urls}" "${output_dir}"
