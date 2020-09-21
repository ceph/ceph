#!/usr/bin/env bash
set -ex

# this should be run from the src directory in the ceph.git (when built with
# automake) or cmake build directory

source $(dirname $0)/detect-build-env-vars.sh

RBD_FEATURES=13 valgrind --tool=memcheck --leak-check=full --error-exitcode=1 \
	    --suppressions=${CEPH_ROOT}/src/valgrind.supp unittest_librbd

echo OK
