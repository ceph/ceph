#!/bin/bash -e

source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

dir=$1

set -e

tmp1=`mktemp /tmp/typ-XXXXXXXXX`
tmp2=`mktemp /tmp/typ-XXXXXXXXX`
tmp3=`mktemp /tmp/typ-XXXXXXXXX`
tmp4=`mktemp /tmp/typ-XXXXXXXXX`

failed=0
numtests=0
echo "checking ceph-dencoder generated test instances..."
echo "numgen type"
for type in `ceph-dencoder list_types`; do
    num=`ceph-dencoder type $type count_tests`
    echo "$num $type"
    for n in `seq 1 1 $num 2>/dev/null`; do
	safe_type=$type
	# BitVector<2> needs some escaping to avoid bash issues with <>
	if [ "$type" = "BitVector<2>" ]; then
	    safe_type="BitVector\<2\>"
	fi

	pids=""
	run_in_background pids bash -c "ceph-dencoder type $safe_type select_test $n dump_json > $tmp1"
	run_in_background pids bash -c "ceph-dencoder type $safe_type select_test $n encode decode dump_json > $tmp2"
	run_in_background pids bash -c "ceph-dencoder type $safe_type select_test $n copy dump_json > $tmp3"
	run_in_background pids bash -c "ceph-dencoder type $safe_type select_test $n copy_ctor dump_json > $tmp4"
	wait_background pids

	if [ $? -ne 0 ]; then
	    echo "**** $type test $n encode+decode check failed ****"
	    echo "   ceph-dencoder type $type select_test $n encode decode"
	    failed=$(($failed + 3))
	    continue
	fi

	# nondeterministic classes may dump nondeterministically.  compare
	# the sorted json output.  this is a weaker test, but is better
	# than nothing.
	deterministic=0
	if ceph-dencoder type $type is_deterministic; then
	    deterministic=1
	fi

	if [ $deterministic -eq 0 ]; then
	    echo "  sorting json output for nondeterministic object"
	    for f in $tmp1 $tmp2 $tmp3 $tmp4; do
		sort $f | sed 's/,$//' > $f.new
		mv $f.new $f
	    done
	fi

	if ! cmp $tmp1 $tmp2; then
	    echo "**** $type test $n dump_json check failed ****"
	    echo "   ceph-dencoder type $type select_test $n dump_json > $tmp1"
	    echo "   ceph-dencoder type $type select_test $n encode decode dump_json > $tmp2"
	    echo "   diff $tmp1 $tmp2"
	    failed=$(($failed + 1))
	fi

	if ! cmp $tmp1 $tmp3; then
	    echo "**** $type test $n copy dump_json check failed ****"
	    echo "   ceph-dencoder type $type select_test $n dump_json > $tmp1"
	    echo "   ceph-dencoder type $type select_test $n copy dump_json > $tmp2"
	    echo "   diff $tmp1 $tmp2"
	    failed=$(($failed + 1))
	fi

	if ! cmp $tmp1 $tmp4; then
	    echo "**** $type test $n copy_ctor dump_json check failed ****"
	    echo "   ceph-dencoder type $type select_test $n dump_json > $tmp1"
	    echo "   ceph-dencoder type $type select_test $n copy_ctor dump_json > $tmp2"
	    echo "   diff $tmp1 $tmp2"
	    failed=$(($failed + 1))
	fi

	if [ $deterministic -ne 0 ]; then
	    run_in_background pids bash -c "ceph-dencoder type $safe_type select_test $n encode export $tmp1"
	    run_in_background pids bash -c "ceph-dencoder type $safe_type select_test $n encode decode encode export $tmp2"
	    wait_background pids

	    if ! cmp $tmp1 $tmp2; then
		echo "**** $type test $n binary reencode check failed ****"
		echo "   ceph-dencoder type $type select_test $n encode export $tmp1"
		echo "   ceph-dencoder type $type select_test $n encode decode encode export $tmp2"
		echo "   cmp $tmp1 $tmp2"
		failed=$(($failed + 1))
	    fi
	fi

	numtests=$(($numtests + 3))
    done
done

rm -f $tmp1 $tmp2 $tmp3 $tmp4

if [ $failed -gt 0 ]; then
    echo "FAILED $failed / $numtests tests."
    exit 1
fi
echo "passed $numtests tests."
