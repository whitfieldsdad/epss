#!/bin/bash

source util.sh

input_dir=$1

if [[ -z ${input_dir} ]]; then
    echo "Usage: $0 <input_dir>"
    exit 1
fi

# Get list of files, sort lexically, first file is oldest, last file is newest
files=($(ls ${input_dir} | sort))

# Get length of array
num_files=${#files[@]}
oldest_file=${files[0]}
newest_file=${files[$((num_files-1))]}

min_date=$(get_date_from_filename ${oldest_file})
max_date=$(get_date_from_filename ${newest_file})

echo "${min_date},${max_date}"
