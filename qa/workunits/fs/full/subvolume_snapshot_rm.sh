#!/usr/bin/env bash
set -ex

# This testcase tests the  'ceph fs subvolume snapshot rm' when the osd is full.
# The snapshot rm fails with 'MetadataMgrException: -28 (error in write)' and
# truncates the config file of corresponding subvolume. Hence the subsequent
# snapshot rm of the same snapshot fails with 'MetadataMgrException: -2 (section 'GLOBAL' does not exist)'
# traceback.

# The osd is of the size 2GiB. The subvolume is created and 1.6GB file is written.
# Then full-ratios are set below 1GiB such that the osd is treated as full.
# The subvolume snapshot is taken which succeeds as no extra space is required
# for snapshot. Now, the removal of the snapshot fails with ENOSPACE as it
# fails to remove the snapshot metadata set. The snapshot removal fails
# but should not traceback and truncate the config file.

set -e
expect_failure() {
	if "$@"; then return 1; else return 0; fi
}

ignore_failure() {
	if "$@"; then return 0; else return 0; fi
}

ceph fs subvolume create cephfs sub_0
subvol_path=$(ceph fs subvolume getpath cephfs sub_0 2>/dev/null)

#For debugging
echo "Before write"
df $CEPH_MNT
ceph osd df

# Write 1.6GB file and set full ratio to around 400MB
ignore_failure sudo dd if=/dev/urandom of=$CEPH_MNT$subvol_path/1.6GB_file-1 status=progress bs=1M count=1600 conv=fdatasync

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

#Take snapshot
ceph fs subvolume snapshot create cephfs sub_0 snap_0

#Remove snapshot fails but should not throw traceback
expect_failure ceph fs subvolume snapshot rm cephfs sub_0 snap_0 2>/tmp/error_${PID}_file
cat /tmp/error_${PID}_file

# No traceback should be found
expect_failure grep "Traceback" /tmp/error_${PID}_file

# Validate config file is not truncated and GLOBAL section exists
sudo grep "GLOBAL" $CEPH_MNT/volumes/_nogroup/sub_0/.meta

#For debugging
echo "After write"
df $CEPH_MNT
ceph osd df

# Snapshot removal with force option should succeed
ceph fs subvolume snapshot rm cephfs sub_0 snap_0 --force

#Cleanup from backend
ignore_failure sudo rm -f /tmp/error_${PID}_file
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
