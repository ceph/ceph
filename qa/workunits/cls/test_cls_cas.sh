#!/bin/sh -e

GTEST_FILTER=${CLS_CAS_GTEST_FILTER:-*}
ceph_test_cls_cas --gtest_filter=${GTEST_FILTER}

exit 0
