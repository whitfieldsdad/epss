#!/bin/bash

source epss.sh

input_file=$1

if [[ -z ${input_file} ]]; then
    echo "Usage: $0 <input_file>"
    exit 1
fi

# Check if the input file is a directory
if [[ -d ${input_file} ]]; then
    latest_file=$(ls -1 ${input_file} | tail -n 1)
    latest_date=$(get_date_from_filename ${latest_file})
    
    # Get the latest available date
    max_date=$(get_max_date)

    
    if [[ ${latest_date} < ${max_date} ]]; then
        diff=$(get_date_diff ${latest_date} ${max_date})
        echo "Local repository is ${diff} days behind the latest available data"
    fi

else 
    echo "Input file ${input_file} is not a directory"
fi
