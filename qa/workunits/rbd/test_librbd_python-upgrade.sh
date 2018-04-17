#!/bin/sh -ex

relpath=$(dirname $0)/../../../src/test/pybind

# in mimic we change the default features
EXCLUDE="-e test_rbd.test_create_defaults"

if [ -n "${VALGRIND}" ]; then
  valgrind ${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    --errors-for-leak-kinds=definite --error-exitcode=1 \
    nosetests -v $EXCLUDE $relpath/test_rbd.py
else
  nosetests -v $EXCLUDE $relpath/test_rbd.py 
fi
exit 0
