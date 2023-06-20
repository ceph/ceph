#!/bin/sh -ex

relpath=$(dirname $0)/../../../src/test/pybind

if [ -n "${VALGRIND}" ]; then
  valgrind ${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    --errors-for-leak-kinds=definite --error-exitcode=1 \
    python3 -m pytest -v $relpath/test_rbd.py
else
    python3 -m pytest -v $relpath/test_rbd.py
fi
exit 0
