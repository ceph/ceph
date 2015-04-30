#!/bin/sh -e

if [ -n "${VALGRIND}" ]; then
  valgrind --tool=${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    ceph_test_librbd
else
  ceph_test_librbd
fi
exit 0
