#!/bin/sh -e

dir=$1

set -e

tmp1=`mktemp /tmp/typ-XXXXXXXXX`
tmp2=`mktemp /tmp/typ-XXXXXXXXX`

for type in `ls $dir`
do
    if ./ceph-dencoder type $type 2>/dev/null; then
        echo "type $type"
	for o in `ls $dir/$type`; do
	    f="$dir/$type/$o"
	    echo "\t$f"

            ./ceph-dencoder type $type import $f decode dump_json > $tmp1
            ./ceph-dencoder type $type import $f decode encode decode dump_json > $tmp2
            cmp $tmp1 $tmp2 || exit 1

            ./ceph-dencoder type $type import $f decode encode export $tmp1
            cmp $tmp1 $f || exit 1
	done
    else
        echo "skip $type"
    fi
done

rm -f $tmp1 $tmp2

echo OK
