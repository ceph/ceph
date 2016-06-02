#!/bin/sh -e

if [ -n "${VALGRIND}" ]; then
  valgrind --tool=${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    --error-limit=no --error-exitcode=140 ${VALGRIND_ARGUMENTS} ceph_test_librbd
else
  ceph_test_librbd
fi
exit 0
