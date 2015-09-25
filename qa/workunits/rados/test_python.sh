#!/bin/sh -ex

CEPH_REF=${CEPH_REF:-master}
#wget -q https://raw.github.com/ceph/ceph/$CEPH_REF/src/test/pybind/test_rados.py
wget -O test_rados.py "https://git.ceph.com/?p=ceph.git;a=blob_plain;hb=$CEPH_REF;f=src/test/pybind/test_rados.py" || \
    wget -O test_rados.py "https://git.ceph.com/?p=ceph.git;a=blob_plain;hb=ref/heads/$CEPH_REF;f=src/test/pybind/test_rados.py"
nosetests -v test_rados
exit 0
