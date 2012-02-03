#!/bin/sh -e

src=$1
ver=$2
archive=$3

[ -d "$archive" ] && [ -d "$src" ] || echo "usage: $0 <srcdir> <version> <archive>"

[ -d "$archive/$ver" ] || mkdir "$archive/$ver"

for f in `find $src -type f`
do
    n=`basename $f`
    type=`echo $n | sed 's/__.*//'`
    md=`md5sum $f | awk '{print $1}'`

    [ -d "$archive/$ver/$type" ] || mkdir $archive/$ver/$type
    [ -e "$archive/$ver/$type/$md" ] || cp $f $archive/$ver/$type/$md
done