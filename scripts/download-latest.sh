#!/bin/bash

source util.sh

output_dir=$1
if [[ -z ${output_dir} ]]; then
    echo "Usage: $0 <output_dir>"
    exit 1
fi
output_dir=${output_dir%/}
mkdir -p ${output_dir}

url=$(get_download_url)
path=${output_dir}/$(basename ${url})

if [[ -f ${path} ]]; then
    echo "File ${path} already exists"
    exit 0
fi

download_file "${url}" "${path}"
