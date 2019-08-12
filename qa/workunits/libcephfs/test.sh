#!/bin/sh -e

ceph_test_libcephfs
ceph_test_libcephfs_access
ceph_test_libcephfs_reclaim

exit 0
