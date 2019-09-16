#!/bin/bash
#
# args: osd.id to be replaced

set -ex

OSD_ID=$1
ceph osd tree --format json | tee before.json
OSD_PATH=$(salt \* cephdisks.find_by_osd_id ${OSD_ID} --out json 2> /dev/null | jq -j '.[][].path')
export OSD_PATH
salt-run osd.replace ${OSD_ID} 2> /dev/null
systemctl status ceph-osd@${OSD_ID} -l --no-pager || true # prevent premature exit
sgdisk -Z ${OSD_PATH}
sgdisk -p ${OSD_PATH}
lsblk
lsof /var/lib/ceph/osd/ceph-${OSD_ID} || true
sync
mkfs.ext4 -F ${OSD_PATH}
mount ${OSD_PATH} /mnt
ceph-volume inventory
salt-run disks.c_v_commands 2>/dev/null
