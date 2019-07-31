#!/bin/sh -e

archive=$1

[ -d "$archive" ] || echo "usage: $0 <archive>"

ver=`bin/ceph-dencoder version`
echo "version $ver"

[ -d "$archive/$ver" ] || mkdir "$archive/$ver"

tmp1=`mktemp /tmp/typ-XXXXXXXXX`

echo "numgen\ttype"
for type in `bin/ceph-dencoder list_types`; do

    [ -d "$archive/$ver/objects/$type" ] || mkdir -p "$archive/$ver/objects/$type"

    num=`bin/ceph-dencoder type $type count_tests`
    echo "$num\t$type"
    max=$(($num - 1))
    for n in `seq 0 $max`; do
	bin/ceph-dencoder type $type select_test $n encode export $tmp1
	md=`md5sum $tmp1 | awk '{print $1}'`
	echo "\t$md"
	[ -e "$archive/$ver/objects/$type/$md" ] || cp $tmp1 $archive/$ver/objects/$type/$md
    done
done

rm $tmp1
