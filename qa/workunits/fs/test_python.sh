#!/bin/sh -ex

python3 -m pytest -v $(dirname $0)/../../../src/test/pybind/test_cephfs.py
exit 0
