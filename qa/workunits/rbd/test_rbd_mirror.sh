#!/bin/sh -e

if [ -n "${VALGRIND}" ]; then
  valgrind --tool=${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    ceph_test_rbd_mirror
else
  ceph_test_rbd_mirror
fi
exit 0
