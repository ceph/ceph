#!/usr/bin/env bash
set -ex

# This testcase tests the  'ceph fs subvolume snapshot clone' when the osd is full.
# The clone fails with 'MetadataMgrException: -28 (error in write)' and
# truncates the config file of corresponding subvolume while updating the config file.
# Hence the subsequent subvolume commands on the clone fails with
# 'MetadataMgrException: -2 (section 'GLOBAL' does not exist)' traceback.

# The osd is of the size 2GiB. The full-ratios are set so that osd is treated full
# at around 1.2GB. The subvolume is created and 200MB is written.
# The subvolume is snapshotted and cloned ten times. Since the clone delay is set to 15 seconds,
# all the clones reach pending state for sure. Among ten clones, only few succeed and rest fails
# with ENOSPACE.

# At this stage, ".meta" config file of the failed clones are checked if it's truncated.
# and clone status command is checked for traceback.

# Note that the failed clones would be in retry loop and it's state would be 'pending' or 'in-progress'.
# It's state is not updated to 'failed' as the config update to gets ENOSPACE too.

ignore_failure() {
        if "$@"; then return 0; else return 0; fi
}

expect_failure() {
        if "$@"; then return 1; else return 0; fi
}

NUM_CLONES=10

ceph fs subvolume create cephfs sub_0
subvol_path_0=$(ceph fs subvolume getpath cephfs sub_0 2>/dev/null)

# For debugging
echo "Before ratios are set"
df $CEPH_MNT
ceph osd df

ceph osd set-full-ratio 0.6
ceph osd set-nearfull-ratio 0.50
ceph osd set-backfillfull-ratio 0.55

# For debugging
echo "After ratios are set"
df -h
ceph osd df

for i in {1..100};do sudo dd if=/dev/urandom of=$CEPH_MNT$subvol_path_0/2MB_file-$i status=progress bs=1M count=2 conv=fdatasync;done

# For debugging
echo "After subvolumes are written"
df -h $CEPH_MNT
ceph osd df

# snapshot
ceph fs subvolume snapshot create cephfs sub_0 snap_0

# Set clone snapshot delay
ceph config set mgr mgr/volumes/snapshot_clone_delay 15

# Schedule few clones, some would fail with no space
for i in $(eval echo {1..$NUM_CLONES});do ceph fs subvolume snapshot clone cephfs sub_0 snap_0 clone_$i;done

# Wait for osd is full
timeout=90
while [ $timeout -gt 0 ]
do
  health=$(ceph health detail)
  [[ $health = *"OSD_FULL"* ]] && echo "OSD is full" && break
  echo "Wating for osd to be full: $timeout"
  sleep 1
  let "timeout-=1"
done

# For debugging
echo "After osd is full"
df -h $CEPH_MNT
ceph osd df

# Check clone status, this should not crash
for i in $(eval echo {1..$NUM_CLONES})
do
  ignore_failure ceph fs clone status cephfs clone_$i >/tmp/out_${PID}_file 2>/tmp/error_${PID}_file
  cat /tmp/error_${PID}_file
  if grep "complete" /tmp/out_${PID}_file; then
    echo "The clone_$i is completed"
  else
    #in-progress/pending clones, No traceback should be found in stderr
    echo clone_$i in PENDING/IN-PROGRESS
    expect_failure sudo grep "Traceback" /tmp/error_${PID}_file
    #config file should not be truncated and GLOBAL section should be found
    sudo grep "GLOBAL" $CEPH_MNT/volumes/_nogroup/clone_$i/.meta
  fi
done

# Hard cleanup
ignore_failure sudo rm -rf $CEPH_MNT/_index/clone/*
ignore_failure sudo rm -rf $CEPH_MNT/volumes/_nogroup/clone_*
ignore_failure sudo rmdir $CEPH_MNT/volumes/_nogroup/sub_0/.snap/snap_0
ignore_failure sudo rm -rf $CEPH_MNT/volumes/_nogroup/sub_0

#Set the ratios back for other full tests to run
ceph osd set-full-ratio 0.95
ceph osd set-nearfull-ratio 0.95
ceph osd set-backfillfull-ratio 0.95

#After test
echo "After test"
df -h $CEPH_MNT
ceph osd df

echo OK
