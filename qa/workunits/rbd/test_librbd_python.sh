#!/bin/sh -ex

relpath=$(dirname $0)/../../../src/test/pybind
nosetests -v $relpath/test_rbd.py
exit 0
