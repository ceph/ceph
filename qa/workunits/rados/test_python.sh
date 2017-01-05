#!/bin/sh -ex

${PYTHON:-python} -m nose -v $(dirname $0)/../../../src/test/pybind/test_rados.py
exit 0
