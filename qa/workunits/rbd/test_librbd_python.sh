#!/bin/sh -ex

relpath=$(dirname $0)/../../../src/test/pybind

if [ -n "${VALGRIND}" ]; then
  valgrind --tool=${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    nosetests -v $relpath/test_rbd
else
  nosetests -v $relpath/test_rbd
fi
exit 0
