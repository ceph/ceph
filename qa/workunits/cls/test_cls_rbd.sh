#!/bin/sh -e

ceph_test_cls_rbd --gtest_filter=-TestClsRbd.parents

exit 0
