#!/bin/sh -e

dir=$1

set -e

tmp1=`mktemp /tmp/typ-XXXXXXXXX`
tmp2=`mktemp /tmp/typ-XXXXXXXXX`

failed=0
numtests=0

for vdir in $dir/*
do
#    echo $vdir
    for type in `ls $vdir`
    do
	if ./ceph-dencoder type $type 2>/dev/null; then
#	    echo "type $type";
	    echo "\t$vdir/$type"
	    for f in `ls $vdir/$type`; do
#		echo "\t$vdir/$type/$f"
		if ! ./ceph-dencoder type $type import $vdir/$type/$f decode dump_json > $tmp1; then
		    echo "**** failed to decode $vdir/$type/$f ****"
		    failed=$(($failed + 1))
		    continue	    
		fi
		if ! ./ceph-dencoder type $type import $vdir/$type/$f decode encode decode dump_json > $tmp2; then
		    echo "**** failed to decode+encode+decode $vdir/$type/$f ****"
		    failed=$(($failed + 1))
		    continue
		fi
		if ! cmp $tmp1 $tmp2; then
		    echo "**** reencode of $vdir/$type/$f resulted in a different dump ****"
		    diff $tmp1 $tmp2
		    failed=$(($failed + 1))
	    	fi
		numtests=$(($numtests + 1))
	    done
	else
            echo "skipping unrecognized type $type"
	fi
    done
done

rm -f $tmp1 $tmp2

if [ $failed -gt 0 ]; then
    echo "FAILED $failed / $numtests tests."
    exit 1
fi
echo "passed $numtests tests."

