#!/bin/sh -ex

./ceph_test_objectstore --gtest_filter=\*/0

echo OK
