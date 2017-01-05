#!/bin/sh -ex

relpath=$(dirname $0)/../../../src/test/pybind

if [ -n "${VALGRIND}" ]; then
  valgrind --tool=${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    nosetests -v $relpath/test_rbd.py
else
  nosetests -v $relpath/test_rbd.py
fi
exit 0
