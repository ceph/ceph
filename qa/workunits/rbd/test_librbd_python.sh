#!/bin/sh -ex

relpath=$(dirname $0)/../../../src/test/pybind

source /etc/os-release

pycmd=$"python3 -m nose"

if [ "$VERSION_ID" == "7.7" ]; then
        pycmd=$"nosetests"
fi

if [ -n "${VALGRIND}" ]; then
  valgrind ${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    --errors-for-leak-kinds=definite --error-exitcode=1 \
    $pycmd -v $relpath/test_rbd.py
else
  $pycmd -v $relpath/test_rbd.py
fi
exit 0
