#!/bin/sh -ex

nosetests -v $(dirname $0)/../../../src/test/pybind/test_rados
exit 0
