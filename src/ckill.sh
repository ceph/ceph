#!/bin/bash -e

if [ -e CMakeCache.txt ]; then
    [ -z "$CEPH_BIN" ] && CEPH_BIN=bin
fi

if [ -z "$CEPHADM" ]; then
    CEPHADM="${CEPH_BIN}/cephadm"
fi

# fsid
if [ -e fsid ] ; then
    fsid=`cat fsid`
else
    echo 'no fsid file, so no cluster?'
    exit 0
fi
echo "fsid $fsid"

sudo $CEPHADM rm-cluster --force --fsid $fsid

