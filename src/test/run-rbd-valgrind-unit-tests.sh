#!/bin/bash -ex

# this should be run from the src directory in the ceph.git

CEPH_SRC=$(pwd)
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$CEPH_SRC/.libs"
PATH="$CEPH_SRC:$PATH"

RBD_FEATURES=13 valgrind --tool=memcheck --leak-check=full --suppressions=valgrind.supp unittest_librbd

echo OK
