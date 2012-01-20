#!/bin/sh -e

dir=$1

set -e

tmp1=`mktemp /tmp/typ-XXXXXXXXX`
tmp2=`mktemp /tmp/typ-XXXXXXXXX`

for type in `ls $dir`
do
    if ./ceph-dencoder type $type 2>/dev/null; then
	echo "type $type";
	for f in `ls $dir/$type`; do
	    echo "\t$dir/$type/$f"
            ./ceph-dencoder type $type import $dir/$type/$f decode dump_json > $tmp1
            ./ceph-dencoder type $type import $dir/$type/$f decode encode decode dump_json > $tmp2
            cmp $tmp1 $tmp2 || exit 1
	done
    else
        echo "skip $type"
    fi
done

rm -f $tmp1 $tmp2

echo OK
