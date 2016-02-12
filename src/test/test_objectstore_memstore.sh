#!/bin/bash -ex

source $(dirname $0)/detect-build-env-vars.sh
rm -rf store_test_temp_dir
$CEPH_BIN/ceph_test_objectstore --gtest_filter=\*/0

echo OK
