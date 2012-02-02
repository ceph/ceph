#!/bin/sh -e

dir=$1

set -e

tmp1=`mktemp /tmp/typ-XXXXXXXXX`
tmp2=`mktemp /tmp/typ-XXXXXXXXX`

echo "checking ceph-dencoder generated test instances..."
echo "numgen type"
for type in `./ceph-dencoder list_types`; do
    num=`./ceph-dencoder type $type count_tests`
    echo "$num $type"
    for n in `seq 1 $num`; do
	./ceph-dencoder type $type select_test $n dump_json > $tmp1
	./ceph-dencoder type $type select_test $n encode decode dump_json > $tmp2
	if ! cmp $tmp1 $tmp2; then
	    echo "$type test $n json check failed"
	    exit 1
	fi

	./ceph-dencoder type $type select_test $n encode export $tmp1
	./ceph-dencoder type $type select_test $n encode decode encode export $tmp2
	if ! cmp $tmp1 $tmp2; then
	    echo "$type test $n binary reencode check failed"
	    exit 1
	fi
    done
done
echo "ok."