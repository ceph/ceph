#!/bin/bash -ex

# 1) run_smr_bluestore_test.sh
# Setup smr device, run all tests

# 2) run_smr_bluestore_test.sh --smr
# Setup smr device but skip tests failing on smr


before_creation=$(mktemp)
lsscsi > $before_creation

echo "cd /backstores/user:zbc
create name=zbc0 size=20G cfgstring=model-HM/zsize-256/conv-10@zbc0.raw
/loopback create
cd /loopback
create naa.50014055e5f25aa0
cd naa.50014055e5f25aa0/luns
create /backstores/user:zbc/zbc0 0
" | sudo targetcli

sleep 1 #if too fast device does not show up
after_creation=$(mktemp)
lsscsi > $after_creation
if [[ $(diff $before_creation $after_creation | wc -l ) != 2 ]]
then
    echo New zbc device not created
    false
fi

function cleanup() {
    echo "cd /loopback
delete naa.50014055e5f25aa0
cd /backstores/user:zbc
delete zbc0" | sudo targetcli
    sudo rm -f zbc0.raw
    rm -f $before_creation $after_creation
}
trap cleanup EXIT

DEV=$(diff $before_creation $after_creation |grep zbc |sed "s@.* /@/@")
sudo chmod 666 $DEV
# Need sudo
# https://patchwork.kernel.org/project/linux-block/patch/20210811110505.29649-3-Niklas.Cassel@wdc.com/
sudo ceph_test_objectstore \
    --bluestore-block-path $DEV \
    --gtest_filter=*/2 \
    $*
