#!/bin/sh -e

dir=$1

set -e

tmp1=`mktemp /tmp/typ-XXXXXXXXX`
tmp2=`mktemp /tmp/typ-XXXXXXXXX`

failed=0
numtests=0

myversion=`./ceph-dencoder version`

for arversion in `ls -v $dir/archive`
do
    vdir="$dir/archive/$arversion"
#    echo $vdir

    if [ ! -d "$vdir/objects" ]; then
	continue;
    fi

    for type in `ls $vdir/objects`
    do
	if ./ceph-dencoder type $type 2>/dev/null; then
#	    echo "type $type";
	    echo "        $vdir/objects/$type"

	    # is there a fwd incompat change between $arversion and $version?
	    incompat=""
	    sawarversion=0
	    for iv in `ls -v $dir/archive`
	    do
		if [ "$iv" = "$arversion" ]; then
		    sawarversion=1
		fi
		if [ $sawarversion -eq 1 ] && [ -e "$dir/archive/$iv/forward_incompat/$type" ]; then
		    incompat="$iv"
		fi
		if [ "$iv" = "$version" ]; then
		    break
		fi
	    done
	    if [ -n "$incompat" ]; then
		echo "skipping incompat $type version $arversion, changed at $iv < code $myversion"
		continue
	    fi

	    for f in `ls $vdir/objects/$type`; do
#		echo "\t$vdir/$type/$f"
		if ! ./ceph-dencoder type $type import $vdir/objects/$type/$f decode dump_json > $tmp1; then
		    echo "**** failed to decode $vdir/objects/$type/$f ****"
		    failed=$(($failed + 1))
		    continue	    
		fi
		if ! ./ceph-dencoder type $type import $vdir/objects/$type/$f decode encode decode dump_json > $tmp2; then
		    echo "**** failed to decode+encode+decode $vdir/objects/$type/$f ****"
		    failed=$(($failed + 1))
		    continue
		fi
		if ! cmp $tmp1 $tmp2; then
		    echo "**** reencode of $vdir/objects/$type/$f resulted in a different dump ****"
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

