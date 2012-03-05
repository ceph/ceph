#!/bin/sh -ex

wget -q https://raw.github.com/ceph/ceph/master/src/test/pybind/test_rbd.py
nosetests -v test_rbd
exit 0
