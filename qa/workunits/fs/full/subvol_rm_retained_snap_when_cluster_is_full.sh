#!/usr/bin/env bash
set -ex

# This testcase tests the scenario of the 'ceph fs subvolume rm --retained-snapshots'
# mgr command when the osd is full.The osd is of the size 2GiB. The subvolume is
# created and 1GB file is written. The full-ratios are set below 500MB such that the
# osd is treated as full. Now the subvolume with retained-snapshots is removed. This
# should fail with the introduction of FULL capabilities which the mgr holds as the
# config update gets ENOSPACE error

set -e
expect_failure() {
	if "$@"; then return 1; else return 0; fi
}

ceph fs subvolume create cephfs sub_0
subvol_path=$(ceph fs subvolume getpath cephfs sub_0 2>/dev/null)

#For debugging
echo "Printing system disk usages for host as well Ceph before writing on subvolume"
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
echo "Printing disk usage for host as well as Ceph after OSD ratios have been set"
df -h
ceph osd df

#Take snapshot
ceph fs subvolume snapshot create cephfs sub_0 snap_0

#Delete subvolume with retain snapshot fails
expect_failure ceph fs subvolume rm cephfs sub_0 --retain-snapshots

#Validate subvolume is not deleted
ceph fs subvolume info cephfs sub_0

# Validate config file is not truncated and GLOBAL section exists
sudo grep "GLOBAL" $CEPH_MNT/volumes/_nogroup/sub_0/.meta

# Hard cleanup
ignore_failure sudo rmdir $CEPH_MNT/volumes/_nogroup/sub_0/.snap/snap_0
ignore_failure sudo rm -rf $CEPH_MNT/volumes/_nogroup/sub_0

#Reset the ratios to original values for the sake of rest of tests
ceph osd set-full-ratio 0.95
ceph osd set-nearfull-ratio 0.95
ceph osd set-backfillfull-ratio 0.95

#For debugging
echo "Printing disk usage for host as well as Ceph since test has been finished"
df -h
ceph osd df

echo OK
