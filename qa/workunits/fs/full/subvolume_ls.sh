#!/usr/bin/env bash
set -ex

# This testcase tests the scenario of the 'ceph fs subvolume ls' mgr command
# when the osd is full. The command used to miss out few subvolumes in the list.
# The issue happens in the multi-mds active setup. Please the tracker
# https://tracker.ceph.com/issues/72260

# The osd is of the size 2GiB. The 10 subvolumes are created and a 1GB
# file is written is on the root and the full-ratios are set below 500MB
# such that the osd is treated as full. Now subvolumes are listed and
# validated for each subvolume being listed.

expect_failure() {
	if "$@"; then return 1; else return 0; fi
}
validate_subvol_cnt() {
	if [ $1 -eq 10 ]; then return 0; else return 1; fi
}

#Set max_mds to 2
ceph fs set cephfs max_mds 2
timeout=30
while [ $timeout -gt 0 ]
do
  active_cnt=$(ceph fs status | grep active | wc -l)
  if [ $active_cnt -eq 2 ]; then break; fi
  echo "Wating for max_mds to be 2: $timeout"
  sleep 1
  let "timeout-=1"
done

#Create 10 subvolumes
for i in {1..10}; do ceph fs subvolume create cephfs sub_$i; done

#For debugging
echo "Before write"
df -h
ceph osd df

sudo dd if=/dev/urandom of=$CEPH_MNT/1GB_file-1 status=progress bs=1M count=1000

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

#Clear readdir cache by failing over mgr which forces to use new libcephfs connection
ceph mgr fail x
timeout=30
while [ $timeout -gt 0 ]
do
  active_mgr_cnt=$(ceph status | grep mgr | grep active | grep -v no | wc -l)
  if [ $active_mgr_cnt -eq 1 ]; then break; fi
  echo "Wating for mgr to be active after failover: $timeout"
  sleep 1
  let "timeout-=1"
done

#List subvolumes
subvol_cnt=$(ceph fs subvolume ls cephfs --format=json-pretty | grep sub_ | wc -l)
validate_subvol_cnt $subvol_cnt

#Delete all subvolumes
for i in {1..10}; do ceph fs subvolume rm cephfs sub_$i; done

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

sudo rm -f $CEPH_MNT/1GB_file-1

#Set the ratios back for other full tests to run
ceph osd set-full-ratio 0.95
ceph osd set-nearfull-ratio 0.95
ceph osd set-backfillfull-ratio 0.95

#After test
echo "After test"
df -h
ceph osd df

echo OK
