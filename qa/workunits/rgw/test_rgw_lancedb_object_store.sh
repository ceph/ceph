#!/bin/sh -e

# libtest owns argv, so ceph configuration comes from the environment; the
# default /etc/ceph/ceph.conf lookup applies when CEPH_CONF is unset.
ceph_test_rgw_lancedb_object_store --nocapture

exit 0
