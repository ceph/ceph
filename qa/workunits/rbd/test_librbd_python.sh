#!/bin/sh -ex

wget -q https://raw.github.com/ceph/ceph/master/src/test/pybind/test_rbd.py
nosetests -v -e '.*test_remove_with_watcher' test_rbd
exit 0
