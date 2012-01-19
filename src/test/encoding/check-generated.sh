#!/bin/sh -e

dir=$1

set -e

tmp1=`mktemp /tmp/typ-XXXXXXXXX`
tmp2=`mktemp /tmp/typ-XXXXXXXXX`

echo "numgen\ttype"
for type in `./ceph-dencoder list_types`; do
    num=`./ceph-dencoder type $type count`
    echo "$num\t$type"
    max=$(($num - 1))
    for n in `seq 0 $max`; do
	./ceph-dencoder type $type select $n dump_json > $tmp1
	./ceph-dencoder type $type select $n encode decode dump_json > $tmp2
	cmp $tmp1 $tmp2 || exit 1

	./ceph-dencoder type $type select $n encode export $tmp1
	./ceph-dencoder type $type select $n encode decode encode export $tmp2
	cmp $tmp1 $tmp2 || exit 1
    done
done