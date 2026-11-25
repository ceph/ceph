#!/bin/sh -ex

# The tests of many threads of one mount and of the client lock options,
# with more threads and longer runs than in test.sh (ceph_test_libcephfs
# runs them once at the default size there).
export CEPH_TEST_THREADS_SCALE=${CEPH_TEST_THREADS_SCALE:-4}
export CEPH_TEST_THREADS_WIDE=${CEPH_TEST_THREADS_WIDE:-2}
ceph_test_libcephfs \
    --gtest_filter='LibCephFSThreads.*:*LibCephFSClientOption.*:LibCephFSClientOptions.*'

exit 0
