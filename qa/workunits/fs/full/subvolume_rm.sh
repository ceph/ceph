#!/usr/bin/env bash
set -ex

# This testcase tests the scenario of the 'ceph fs subvolume rm' mgr command
# when the osd is full. The command used to hang. The osd is of the size 2GiB.
# The subvolume is created and 1GB file is written. The full-ratios are
# set below 500MB such that the osd is treated as full. Now the subvolume is
# is removed. This should be successful with the introduction of FULL
# capabilities which the mgr holds.

set -e
expect_failure() {
	if "$@"; then return 1; else return 0; fi
}

ceph fs subvolume create cephfs sub_0
subvol_path=$(ceph fs subvolume getpath cephfs sub_0 2>/dev/null)

#For debugging
echo "Before write"
df -h
ceph osd df

sudo dd if=/dev/urandom of=$CEPH_MNT$subvol_path/1GB_file-1 status=progress bs=1M count=1000

ceph osd set-full-ratio 0.2
ceph osd set-nearfull-ratio 0.16
ceph osd set-backfillfull-ratio 0.18

timeout=30
while [ $timeout -gt 0 ]
do
  health=$(ceph health detail)
  [[ $health = *"OSD_FULL"* ]] && echo "OSD is full" && break
  echo "Wating for osd to be full: $timeout"
  sleep 1
  let "timeout-=1"
done

#For debugging
echo "After ratio set"
df -h
ceph osd df

#Delete subvolume
ceph fs subvolume rm cephfs sub_0

#Validate subvolume is deleted
expect_failure ceph fs subvolume info cephfs sub_0

#Wait for subvolume to delete data
trashdir=$CEPH_MNT/volumes/_deleting
timeout=30
while [ $timeout -gt 0 ]
do
  [ -z "$(sudo ls -A $trashdir)" ] && echo "Trash directory $trashdir is empty" &&  break
  echo "Wating for trash dir to be empty: $timeout"
  sleep 1
  let "timeout-=1"
done

#Set the ratios back for other full tests to run
ceph osd set-full-ratio 0.95
ceph osd set-nearfull-ratio 0.95
ceph osd set-backfillfull-ratio 0.95

#After test
echo "After test"
df -h
ceph osd df

echo OK
