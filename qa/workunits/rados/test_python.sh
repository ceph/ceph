#!/bin/sh -ex

wget -q https://raw.github.com/ceph/ceph/master/src/test/pybind/test_rados.py
nosetests -v test_rados
exit 0
