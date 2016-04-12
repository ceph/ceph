#!/bin/bash -ex

# this should be run from the src directory in the ceph.git

CEPH_SRC=$(pwd)
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$CEPH_SRC/.libs"
PATH="$CEPH_SRC:$PATH"

unittest_librbd
for i in 0 1 61 109
do
    RBD_FEATURES=$i unittest_librbd
done

echo OK
