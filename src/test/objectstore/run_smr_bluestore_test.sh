#!/bin/bash -ex

echo "cd /backstores/user:zbc
create name=zbc0 size=20G cfgstring=model-HM/zsize-256/conv-10@zbc0.raw
cd /loopback
create naa.50014055e5f25aa0
cd naa.50014055e5f25aa$1/luns
create /backstores/user:zbc/zbc0 0
" | sudo targetcli

function cleanup() {
    echo "cd /loopback
delete naa.50014055e5f25aa$1
cd /backstores/user:zbc
delete zbc0" | sudo targetcli
    sudo rm -f zbc0.raw
}
trap cleanup EXIT

DEV=`lsscsi | grep zbc | awk '{print $7}'`

sudo ceph_test_objectstore \
    --bluestore-block-path $DEV \
    --gtest_filter=*/2 \
    --bluestore-block-db-create \
    --bluestore-block-db-size 1048576000 \
    $*
