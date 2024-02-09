#!/bin/sh -e

# this test case changed in 16.2.6 so had been failing in pacific-p2p upgrades since
ceph_test_cls_cmpomap --gtest_filter=-CmpOmap.cmp_vals_u64_invalid_default

exit 0
