#!/bin/sh -ex

rm -rf store_test_temp_dir
./ceph_test_objectstore --gtest_filter=\*/0

echo OK
