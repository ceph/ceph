#!/bin/bash -ex

parallel=1
[ "$1" = "--serial" ] && parallel=0

color=""
[ -t 1 ] && color="--gtest_color=yes"

function cleanup() {
    pkill -P $$ || true
}
trap cleanup EXIT ERR HUP INT QUIT

pids=""
for f in \
    'api_aio --gtest_filter=-LibRadosAio.RacingRemovePP:*WriteSame*:*CmpExt*:*RoundTrip3*:*RoundTripPP3*:*Quota*:*LibRadosAioEC*' \
    'api_list --gtest_filter=-LibRadosList*.EnumerateObjects*:*ListObjectsError*:*EC*' \
    'api_io --gtest_filter=-*Checksum*:*CmpExt*:*EC*' \
    'api_lock --gtest_filter=-*EC*' \
    'api_misc --gtest_filter=-*WriteSame*:*CmpExt*:*Compare*:*Checksum*:*CloneRange*:*EC*' \
    'api_watch_notify --gtest_filter=-*WatchNotify3*:*EC*' \
    'api_tier --gtest_filter=-*EC*' \
    'api_pool --gtest_filter=-*EC*' \
    'api_snapshots --gtest_filter=-*EC*' \
    'api_stat --gtest_filter=-*EC*' \
    'api_cmd --gtest_filter=-*EC*' \
    'api_c_write_operations --gtest_filter=-*WriteSame*:*CmpExt*:*EC*' \
    'api_c_read_operations --gtest_filter=-*Checksum*:*CmpExt*:*EC*' \
    list_parallel \
    open_pools_parallel \
    delete_pools_parallel \
    watch_notify
do
    if [ $parallel -eq 1 ]; then
	r=`printf '%25s' $f`
	bash -o pipefail -exc "ceph_test_rados_$f $color 2>&1 | tee 'ceph_test_rados_$f.log' | sed \"s/^/$r: /\"" &
	pid=$!
	echo "test $f on pid $pid"
	pids="$pids $pid"
    else
	ceph_test_rados_$f
    fi
done

ret=0
if [ $parallel -eq 1 ]; then
for p in $pids
do
  if ! wait $p
  then
    echo "error in $p"
    ret=1
  fi
done
fi

exit $ret
