#!/bin/sh -e

# Integration tests for cls_rgw_ratelimit and the RADOS rate-limit store.
# Requires a running teuthology cluster with cls_rgw_ratelimit loaded on OSDs.
ceph_test_cls_rgw_ratelimit
ceph_test_rgw_ratelimit_rados_store

exit 0
