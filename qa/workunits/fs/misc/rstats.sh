#!/usr/bin/env bash

set -x

timeout=30
old_value=""
new_value=""

wait_until_changed() {
	name=$1
	wait=0
	while [ $wait -lt $timeout ]; do
		new_value=`getfattr --only-value -n ceph.dir.$name .`
		[ $new_value == $old_value ] || return 0
		sleep 1
		wait=$(($wait + 1))
	done
	return 1
}

check_rctime() {
	old_sec=$(echo $old_value | cut -d. -f1)
	old_nsec=$(echo $old_value | cut -d. -f2)
	new_sec=$(echo $new_value | cut -d. -f1)
	new_nsec=$(echo $new_value | cut -d. -f2)
	[ "$old_sec" -lt "$new_sec" ] && return 0
	[ "$old_sec" -gt "$new_sec" ] && return 1
	[ "$old_nsec" -lt "$new_nsec" ] && return 0
	return 1
}

# sync(3) does not make ceph-fuse flush dirty caps, because fuse kernel module
# does not notify ceph-fuse about it. Use fsync(3) instead.
fsync_path() {
	cmd="import os; fd=os.open(\"$1\", os.O_RDONLY); os.fsync(fd); os.close(fd)"
	python3 -c "$cmd"
}

set -e

mkdir -p rstats_testdir/d1/d2
cd rstats_testdir

# rfiles
old_value=`getfattr --only-value -n ceph.dir.rfiles .`
[ $old_value == 0 ] || false
touch d1/d2/f1
wait_until_changed rfiles
[ $new_value == $(($old_value + 1)) ] || false

# rsubdirs
old_value=`getfattr --only-value -n ceph.dir.rsubdirs .`
[ $old_value == 3 ] || false
mkdir d1/d2/d3
wait_until_changed rsubdirs
[ $new_value == $(($old_value + 1)) ] || false

# rbytes
old_value=`getfattr --only-value -n ceph.dir.rbytes .`
[ $old_value == 0 ] || false
echo hello > d1/d2/f2
fsync_path d1/d2/f2
wait_until_changed rbytes
[ $new_value == $(($old_value + 6)) ] || false

#rctime
old_value=`getfattr --only-value -n ceph.dir.rctime .`
touch d1/d2/d3 # touch existing file
fsync_path d1/d2/d3
wait_until_changed rctime
check_rctime

old_value=`getfattr --only-value -n ceph.dir.rctime .`
touch d1/d2/f3 # create new file
wait_until_changed rctime
check_rctime

old_value=`getfattr --only-value -n ceph.dir.rctime .`
setfattr -n "ceph.dir.rctime" -v `date -d 'now+1hour' +%s` .
wait_until_changed rctime
check_rctime

cd ..
rm -rf rstats_testdir
echo OK
