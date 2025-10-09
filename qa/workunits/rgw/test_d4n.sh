#!/bin/sh -e

# run d4n workunits that depend on a running redis server
ceph_test_rgw_d4n_directory
ceph_test_rgw_d4n_policy
ceph_test_rgw_redis_driver
ceph_test_rgw_ssd_driver

exit 0
