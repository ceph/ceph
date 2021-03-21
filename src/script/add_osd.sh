#!/usr/bin/env bash

set -ex

CEPH_DEV_DIR=dev
CEPH_BIN=bin
ceph_adm=$CEPH_BIN/ceph
osd=$1
location=$2
weight=.0990

# DANGEROUS
rm -rf $CEPH_DEV_DIR/osd$osd
mkdir -p $CEPH_DEV_DIR/osd$osd

uuid=`uuidgen`
echo "add osd$osd $uuid"
OSD_SECRET=$($CEPH_BIN/ceph-authtool --gen-print-key)
echo "{\"cephx_secret\": \"$OSD_SECRET\"}" > $CEPH_DEV_DIR/osd$osd/new.json
$CEPH_BIN/ceph osd new $uuid -i $CEPH_DEV_DIR/osd$osd/new.json
rm $CEPH_DEV_DIR/osd$osd/new.json
$CEPH_BIN/ceph-osd -i $osd $ARGS --mkfs --key $OSD_SECRET --osd-uuid $uuid

key_fn=$CEPH_DEV_DIR/osd$osd/keyring
cat > $key_fn<<EOF
[osd.$osd]
	key = $OSD_SECRET
EOF
echo adding osd$osd key to auth repository
$CEPH_BIN/ceph -i "$key_fn" auth add osd.$osd osd "allow *" mon "allow profile osd" mgr "allow profile osd"

$CEPH_BIN/ceph osd crush add osd.$osd $weight $location

echo start osd.$osd
$CEPH_BIN/ceph-osd -i $osd $ARGS $COSD_ARGS
