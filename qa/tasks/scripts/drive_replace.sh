#!/bin/bash
#
# args: osd.id to be replaced

set -ex

OSD_ID="$1"

# write "ceph osd tree" to a file for later "before-after" comparison
ceph osd tree | tee before.txt

# assert that the OSD's systemd unit is active
if systemctl is-active "ceph-osd@$OSD_ID" ; then
    echo "OSD ${OSD_ID} systemd unit is active, as expected" 2>&1 > /dev/null
else
    echo "OSD ${OSD_ID} is to be removed, yet its systemd unit is not active!" 2>&1 > /dev/null
    exit 1
fi

# get the device path of the OSD's underlying disk
OSD_PATH=$(salt \* cephdisks.find_by_osd_id ${OSD_ID} --out json 2> /dev/null | jq -j '.[][].path')

# run DeepSea's "osd.replace" runner
salt-run osd.replace "$OSD_ID" 2> /dev/null

# display OSD tree in log for visual confirmation that OSD is destroyed
ceph osd tree

# assert that the OSD's systemd unit is not active
if systemctl is-active "ceph-osd@$OSD_ID" ; then
    echo "OSD ${OSD_ID} systemd unit is still active, yet OSD was supposed to have been removed!" 2>&1 > /dev/null
    exit 1
else
    echo "OSD ${OSD_ID} systemd unit no longer active. Good." 2>&1 > /dev/null
fi

# simulate physical disk removal
sgdisk -Z "$OSD_PATH"
sgdisk -p "$OSD_PATH"
lsblk
lsof "/var/lib/ceph/osd/ceph-$OSD_ID" || true
sync
mkfs.ext4 -F "$OSD_PATH"
mount "$OSD_PATH" /mnt

# logging for visual confirmation of sanity
ceph-volume inventory
salt-run disks.c_v_commands 2>/dev/null
