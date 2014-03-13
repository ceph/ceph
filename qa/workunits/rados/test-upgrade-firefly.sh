#!/bin/sh -ex

# this is just like test.sh, but skips 2 tests that do not work when
# run against firefly OSDs (due to OMAP_CMP and CMPXATTR behavior
# change).  it is used by the upgrade/dumpling-x (and similar) test
# suites.

ceph_test_rados_api_aio --gtest_filter=-LibRadosAio.OmapPP
ceph_test_rados_api_io
ceph_test_rados_api_list
ceph_test_rados_api_lock
ceph_test_rados_api_misc --gtest_filter=-LibRadosMisc.Operate1PP
ceph_test_rados_api_pool
ceph_test_rados_api_snapshots
ceph_test_rados_api_stat
ceph_test_rados_api_watch_notify --gtest_filter=-LibRadosWatchNotify.WatchNotifyTimeoutTestPP
ceph_test_rados_api_cmd

ceph_test_rados_list_parallel
ceph_test_rados_open_pools_parallel
ceph_test_rados_delete_pools_parallel
ceph_test_rados_watch_notify

exit 0
