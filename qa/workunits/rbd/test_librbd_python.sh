#!/bin/sh -ex

CEPH_REF=${CEPH_REF:-master}
#wget -q https://raw.github.com/ceph/ceph/$CEPH_REF/src/test/pybind/test_rbd.py
wget -q https://ceph.com/git/?p=ceph.git;a=blob_plain;hb=$CEPH_REF;f=src/test/pybind/test_rbd.py || \
    wget -q https://ceph.com/git/?p=ceph.git;a=blob_plain;hb=ref/heads/$CEPH_REF;f=src/test/pybind/test_rbd.py
nosetests -v -e '.*test_remove_with_watcher' test_rbd
exit 0
