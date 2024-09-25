#!/bin/bash

# This script is to be used after running the credits.sh script (see https://tracker.ceph.com/projects/ceph/wiki/Ceph_contributors_list_maintenance_guide)
# as a helper tool to format the contributer names suitable for a ceph.io blog post (see the bottom of https://ceph.io/en/news/blog/2023/v18-2-0-reef-released/).

# To use:
# ./format_contributors.sh <input file of names copied from credits.sh output>
# Sample input file:
#     1      586 Casey Bodley <cbodley@redhat.com>
#     2      544 John Mulligan <jmulligan@redhat.com>
#     3      395 Zac Dover <zac.dover@proton.me>
#     4      345 Patrick Donnelly <pdonnell@redhat.com>
#     5      276 Matan Breizman <mbreizma@redhat.com>
#     6      218 Xuehan Xu <xuxuehan@qianxin.com>
#     7      212 Samuel Just <sjust@redhat.com>
#     8      198 Adam King <adking@redhat.com>
# ...etc.

# Check if the input is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <input_file>"
    exit 1
fi

input_file="$1"

# Declare an array to hold names
declare -a names

# Read the input file line by line
while IFS= read -r line; do
    # Extract the name (everything after the first two whitespace-separated fields and before the first angle bracket)
    name=$(echo "$line" | sed -E 's/^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]+([^<]+)<.*/\1/')
    # Append the name to the array if it's not empty
    if [[ -n $name ]]; then
        names+=("$name")
    fi
done < "$input_file"

# Sort the names alphabetically
sorted_names=()
for name in "${names[@]}"; do
    inserted=false
    for ((i=0; i<${#sorted_names[@]}; i++)); do
        if [[ "$name" < "${sorted_names[i]}" ]]; then
            sorted_names=("${sorted_names[@]:0:i}" "$name" "${sorted_names[@]:i}")
            inserted=true
            break
        fi
    done
    # If not inserted, add it to the end
    if ! $inserted; then
        sorted_names+=("$name")
    fi
done

# Output the formatted and sorted names with trimmed spaces
for name in "${sorted_names[@]}"; do
    # Trim any extra spaces
    trimmed_name=$(echo "$name" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    echo "$trimmed_name &squf;"
done
