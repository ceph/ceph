#!/bin/sh -e

src=$1
ver=$2
archive=$3

[ -d "$archive" ] && [ -d "$src" ] || echo "usage: $0 <srcdir> <version> <archive>"

[ -d "$archive/$ver" ] || mkdir "$archive/$ver"

dest_dir="$archive/$ver/objects"

[ -d "$dest_dir" ] || mkdir "$dest_dir"

for f in `find $src -type f`
do
    n=`basename $f`
    type=`echo $n | sed 's/__.*//'`
    md=`md5sum $f | awk '{print $1}'`

    [ -d "$dest_dir/$type" ] || mkdir $dest_dir/$type
    [ -e "$dest_dir/$type/$md" ] || cp $f $dest_dir/$type/$md
done
