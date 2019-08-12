#!/bin/sh -e

GTEST_FILTER=${CLS_JOURNAL_GTEST_FILTER:-*}
ceph_test_cls_journal --gtest_filter=${GTEST_FILTER}

exit 0
