#!/bin/bash -ex

# this should be run from the src directory in the ceph.git

source $(dirname $0)/detect-build-env-vars.sh
PATH="$CEPH_BIN:$PATH"

unittest_librbd
for i in 0 1 61 109
do
    RBD_FEATURES=$i unittest_librbd
done

echo OK
