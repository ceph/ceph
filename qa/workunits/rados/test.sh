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
    api_aio api_io api_list api_lock api_misc \
    api_tier api_pool api_snapshots api_stat api_watch_notify api_cmd \
    api_c_write_operations \
    api_c_read_operations \
    api_tmap_migrate \
    list_parallel \
    open_pools_parallel \
    delete_pools_parallel \
    watch_notify
do
    if [ $parallel -eq 1 ]; then
	r=`printf '%25s' $f`
	bash -o pipefail -exc "ceph_test_rados_$f $color 2>&1 | tee ceph_test_rados_$f.log | sed \"s/^/$r: /\"" &
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
