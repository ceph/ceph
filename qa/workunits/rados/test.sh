#!/usr/bin/env bash
set -ex

parallel=1
[ "$1" = "--serial" ] && parallel=0

color=""
[ -t 1 ] && color="--gtest_color=yes"

function cleanup() {
    pkill -P $$ || true
}
trap cleanup EXIT ERR HUP INT QUIT

GTEST_OUTPUT_DIR=${TESTDIR:-$(mktemp -d)}/archive/unit_test_xml_report
mkdir -p $GTEST_OUTPUT_DIR

declare -A pids

for f in \
    api_aio api_aio_pp \
    api_io api_io_pp \
    api_asio api_list \
    api_lock api_lock_pp \
    api_misc api_misc_pp \
    api_tier_pp \
    api_pool \
    api_snapshots api_snapshots_pp \
    api_stat api_stat_pp \
    api_watch_notify api_watch_notify_pp \
    api_cmd api_cmd_pp \
    api_service api_service_pp \
    api_c_write_operations \
    api_c_read_operations \
    api_cls_remote_reads \
    list_parallel \
    open_pools_parallel \
    delete_pools_parallel
do
    if [ $parallel -eq 1 ]; then
	r=`printf '%25s' $f`
	ff=`echo $f | awk '{print $1}'`
	bash -o pipefail -exc "ceph_test_rados_$f --gtest_output=xml:$GTEST_OUTPUT_DIR/$f.xml $color 2>&1 | tee ceph_test_rados_$ff.log | sed \"s/^/$r: /\"" &
	pid=$!
	echo "test $f on pid $pid"
	pids[$f]=$pid
    else
	ceph_test_rados_$f
    fi
done

for f in \
    cls cmd handler_error io list misc pool read_operations snapshots \
    watch_notify write_operations
do
    if [ $parallel -eq 1 ]; then
	r=`printf '%25s' $f`
	ff=`echo $f | awk '{print $1}'`
	bash -o pipefail -exc "ceph_test_neorados_$f $color 2>&1 | tee ceph_test_neorados_$ff.log | sed \"s/^/$r: /\"" &
	pid=$!
	echo "test $f on pid $pid"
	pids[$f]=$pid
    else
	ceph_test_neorados_$f
    fi
done

ret=0
if [ $parallel -eq 1 ]; then
for t in "${!pids[@]}"
do
  pid=${pids[$t]}
  if ! wait $pid
  then
    echo "error in $t ($pid)"
    ret=1
  fi
done
fi

exit $ret
