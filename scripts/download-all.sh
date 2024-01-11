#!/bin/bash

source util.sh

output_dir=$1
if [[ -z ${output_dir} ]]; then
    echo "Usage: $0 <output_dir>"
    exit 1
fi
output_dir=${output_dir%/}
mkdir -p ${output_dir}

min_date=$(get_min_date)
max_date=$(get_max_date)
urls=$(get_download_urls ${min_date} ${max_date})

# Announce how many download URLs we found
total=$(echo ${urls[@]} | wc -w | tr -d ' ')
echo "Found ${total} download URLs"

# Determine which files have already been downloaded
pending_urls=()
for url in ${urls[@]}; do
    path=${output_dir}/$(basename ${url})
    if [[ ! -f ${path} ]]; then
        pending_urls+=(${url})
    fi
done

# Calculate how many files need to be downloaded
pending=$(echo ${pending_urls[@]} | wc -w | tr -d ' ')
echo "Found ${pending} files that need to be downloaded"

# Download the files that haven't been downloaded yet
if [[ ${#pending_urls[@]} -eq 0 ]]; then
    echo "All files have already been downloaded"
    exit 0
fi

download_files "${pending_urls[@]}" "${output_dir}"
