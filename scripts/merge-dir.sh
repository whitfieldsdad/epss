#!/bin/bash

source util.sh

input_dir=$1
output_file=$2

if [[ -z ${input_dir} || -z ${output_file} ]]; then
    echo "Usage: $0 <input_dir> <output_file>"
    exit 1
fi

# Strip trailing slash from input directory path
input_dir=${input_dir%/}

# Remove output file if it already exists
rm -f ${output_file}

# Merge all CSV.GZ files into a CSV file
input_files=$(ls ${input_dir}/*.csv.gz)
total=$(echo ${input_files[@]} | wc -w | tr -d ' ')
echo "Merging ${total} files into ${output_file}"

# Write to an uncompressed file.
target_output_file=${output_file%.gz}
rm -f ${target_output_file}

for input_file in ${input_dir}/*.csv.gz; do
    gunzip -c ${input_file} --keep >> ${target_output_file}
done

# If the output file ends with .gz, compress it
if [[ ${output_file} == *.gz ]]; then
    echo "Compressing ${target_output_file} to ${output_file}"
    gzip -c ${target_output_file} > ${output_file}
    rm ${target_output_file}
fi

echo "Done"