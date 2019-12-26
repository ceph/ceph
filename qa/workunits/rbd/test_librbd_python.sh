#!/bin/sh -ex

source $(dirname $0)/../ceph-helpers-root.sh
install python3-nose

relpath=$(dirname $0)/../../../src/test/pybind

if [ -n "${VALGRIND}" ]; then
  valgrind ${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    --errors-for-leak-kinds=definite --error-exitcode=1 \
    python3 -m nose -v $relpath/test_rbd.py
else
    python3 -m nose -v $relpath/test_rbd.py
fi
exit 0
