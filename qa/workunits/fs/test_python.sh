#!/bin/sh -ex

CEPH_REF=${CEPH_REF:-master}
wget -O test_cephfs.py "https://git.ceph.com/?p=ceph.git;a=blob_plain;hb=$CEPH_REF;f=src/test/pybind/test_cephfs.py" || \
    wget -O test_cephfs.py "https://git.ceph.com/?p=ceph.git;a=blob_plain;hb=ref/heads/$CEPH_REF;f=src/test/pybind/test_cephfs.py"

# Running as root because the filesystem root directory will be
# owned by uid 0, and that's where we're writing.
sudo nosetests -v test_cephfs
exit 0
