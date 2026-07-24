#!/usr/bin/env bash
set -ex

# This testcase tests the scenario of the 'ceph fs subvolume ls' mgr command
# when the osd is full. The command used to miss out few subvolumes in the list.
# The issue happens in the multi-mds active setup. Please see the tracker
# https://tracker.ceph.com/issues/72260

# The suite sets the 'bluestore block size' to 2GiB. So, the osd is of the
# size 2GiB. The 25 subvolumes are created and a 1GB file is written on the
# root. The full-ratios are set such that, the data less than 500MB is
# treated as osd full. Now, subvolumes are listed 20 times with mgr failover
# (to invalidate readdir cache) and validated each time.

SUBVOL_CNT=25

expect_failure() {
  if "$@"; then return 1; else return 0; fi
}
validate_subvol_cnt() {
  if [ $1 -eq $SUBVOL_CNT ]; then return 0; else return 1; fi
}
restart_mgr() {
  ceph mgr fail x
  timeout=30
  while [ $timeout -gt 0 ]
  do
    active_mgr_cnt=$(ceph status | grep mgr | grep active | grep -v no | wc -l)
    if [ $active_mgr_cnt -eq 1 ]; then break; fi
    echo "Waiting for mgr to be active after failover: $timeout"
    sleep 1
    let "timeout-=1"
  done
}

#Set client_use_random_mds
ceph config set client client_use_random_mds true

#Set max_mds to 3
ceph fs set cephfs max_mds 3
timeout=30
while [ $timeout -gt 0 ]
do
  active_cnt=$(ceph fs status | grep active | wc -l)
  if [ $active_cnt -eq 2 ]; then break; fi
  echo "Wating for max_mds to be 2: $timeout"
  sleep 1
  let "timeout-=1"
done

#Create subvolumes
for i in $(seq 1 $SUBVOL_CNT); do ceph fs subvolume create cephfs sub_$i; done

#For debugging
echo "Before write"
df -h
ceph osd df

sudo dd if=/dev/urandom of=$CEPH_MNT/1GB_file-1 status=progress bs=1M count=1000

# The suite (qa/suites/fs/full/tasks/mgr-osd-full.yaml) sets the 'bluestore block size'
# to 2GiB. So, the osd is of the size 2GiB. The full-ratios are set below makes sure
# that the data less than 500MB is treated as osd full.
ceph osd set-full-ratio 0.2
ceph osd set-nearfull-ratio 0.16
ceph osd set-backfillfull-ratio 0.18

timeout=30
while [ $timeout -gt 0 ]
do
  health=$(ceph health detail)
  [[ $health = *"OSD_FULL"* ]] && echo "OSD is full" && break
  echo "Waiting for osd to be full: $timeout"
  sleep 1
  let "timeout-=1"
done

#For debugging
echo "After ratio set"
df -h
ceph osd df

#Clear readdir cache by failing over mgr which forces to use new libcephfs connection
#Validate subvolume ls  20 times
for i in {1..20};
do
  restart_mgr
  #List and validate subvolumes count
  subvol_cnt=$(ceph fs subvolume ls cephfs --format=json-pretty | grep sub_ | wc -l)
  validate_subvol_cnt $subvol_cnt
done

#Delete all subvolumes
for i in $(seq 1 $SUBVOL_CNT); do ceph fs subvolume rm cephfs sub_$i; done

#Wait for subvolume to delete data
trashdir=$CEPH_MNT/volumes/_deleting
timeout=30
while [ $timeout -gt 0 ]
do
  [ -z "$(sudo ls -A $trashdir)" ] && echo "Trash directory $trashdir is empty" &&  break
  echo "Waiting for trash dir to be empty: $timeout"
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
