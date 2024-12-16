#!/usr/bin/env bash
set -ex
CEPH_ARGS=""
mydir=`dirname $0`
ceph-dencoder version

# clone the corpus repository on the host
git clone -b master https://github.com/ceph/ceph-object-corpus.git $CEPH_MNT/client.0/tmp/ceph-object-corpus-master

$mydir/test_readable.py $CEPH_MNT/client.0/tmp/ceph-object-corpus-master

echo $0 OK
