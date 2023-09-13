#!/bin/bash -ex

OSD_SECRET=`bin/ceph-authtool --gen-print-key`
echo "{\"cephx_secret\": \"$OSD_SECRET\"}" > /tmp/$$
OSD_UUID=`uuidgen`
OSD_ID=`bin/ceph osd new $OSD_UUID -i /tmp/$$`
rm /tmp/$$
rm dev/osd$OSD_ID/* || true
mkdir -p dev/osd$OSD_ID
bin/ceph-osd -i $OSD_ID --mkfs --key $OSD_SECRET --osd-uuid $OSD_UUID
echo "[osd.$OSD_ID]
key = $OSD_SECRET" > dev/osd$OSD_ID/keyring
H=`hostname`
echo "[osd.$OSD_ID]
host = $H" >> ceph.conf
