#!/bin/sh -e

src=$1
ver=$2
archive=$3

[ -d "$archive" ] && [ -d "$src" ] || echo "usage: $0 <srcdir> <version> <archive>"

mkdir -p "$archive/$ver"

dest_base="$archive/$ver/objects"

mkdir -p "$dest_base"

find "$src" -type f -exec md5sum {} + | \
while read -r md_hash path; do
    filename=$(basename "$path")
    prefix=$(echo "$filename" | cut -d'_' -f1)
    dest_dir="$dest_base/$prefix"

    mkdir -p "$dest_dir"
    [ -e "$dest_dir/$md_hash" ] || cp "$path" "$dest_dir/$md_hash"
done
