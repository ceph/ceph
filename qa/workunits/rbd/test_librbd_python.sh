#!/bin/sh -ex

relpath=$(dirname $0)/../../../src/test/pybind

if [ -n "${VALGRIND}" ]; then
  valgrind ${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    --errors-for-leak-kinds=definite --error-exitcode=1 \
    nosetests -v $relpath/test_rbd.py
else
  nosetests -v $relpath/test_rbd.py
fi
exit 0
