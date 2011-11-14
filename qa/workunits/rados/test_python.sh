#!/bin/sh -ex

wget -q https://raw.github.com/NewDreamNetwork/ceph/master/src/test/pybind/test_rados.py
wget -q https://raw.github.com/NewDreamNetwork/ceph/master/src/test/pybind/test_rgw.py
nosetests -v test_rados
nosetests -v test_rgw
exit 0
