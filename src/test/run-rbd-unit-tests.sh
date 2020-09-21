#!/usr/bin/env bash
set -ex

# this should be run from the src directory in the ceph.git

source $(dirname $0)/detect-build-env-vars.sh
PATH="$CEPH_BIN:$PATH"

if [ $# = 0 ]; then
  # mimic the old behaviour
  TESTS='0 1 61 109 127'
  unset RBD_FEATURES; unittest_librbd
elif [ $# = 1 -a "${1}" = N ] ; then
  # new style no feature request
  unset RBD_FEATURES; unittest_librbd
else 
  TESTS="$*"
fi

for i in ${TESTS}
do
    RBD_FEATURES=$i unittest_librbd
done

echo OK
