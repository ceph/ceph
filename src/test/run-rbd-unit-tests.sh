#!/usr/bin/env bash
set -ex

# this should be run from the src directory in the ceph.git

source $(dirname $0)/detect-build-env-vars.sh
PATH="$CEPH_BIN:$PATH"

# https://tracker.ceph.com/issues/46875
GTEST_FILTER="-TestLibRBD.TestPendingAio"
# https://tracker.ceph.com/issues/49111
GTEST_FILTER+=":TestLibRBD.QuiesceWatchError"
# https://tracker.ceph.com/issues/70691
GTEST_FILTER+=":TestLibRBD.ConcurrentOperations"
# https://tracker.ceph.com/issues/70847
GTEST_FILTER+=":TestMigration.Stress*"

# Optional 2nd arg: a gtest filter so the suite can be split across parallel
# ctest jobs. Each job still runs its cases serially (in-process order matters).
if [ -n "${2:-}" ]; then
  case "$2" in
    *-*) GTEST_FILTER="${2}:${GTEST_FILTER#-}" ;;  # shard already opens a '-' negative section
    *)   GTEST_FILTER="${2}${GTEST_FILTER}" ;;     # positive-only shard; GTEST_FILTER opens the negative section
  esac
fi

# exclude tests that are prone to sporadic failures
export GTEST_FILTER

if [ $# = 0 ]; then
  # mimic the old behaviour
  TESTS='0 1 61 109 127'
  unset RBD_FEATURES; unittest_librbd
elif [ "${1}" = N ] ; then
  # new style no feature request
  unset RBD_FEATURES; unittest_librbd
else
  TESTS="$1"
fi

for i in ${TESTS}
do
    RBD_FEATURES=$i unittest_librbd
done

echo OK
