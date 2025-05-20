#!/usr/bin/env bash
# 
# Author: Max Wang <wangzechen@inspur.com>
#
# This project is used to create a 400G data snapshot and delete it.
# Calculate the delay of snapshot deletion in the osd log.
# The result is saved in ./test/result.log

function run() {
    # Start 10 OSDs using vstart
    cd ../../build
    rm -rf ./test/result.log
    MON=1 OSD=1 MDS=1 RGW=1 ../src/vstart.sh -n --without-dashboard

    # Create snap
    ceph osd pool create snap-test-pool 256 256
    rbd -p snap-test-pool create --image rbd-test-0 --size 400G
    rbd -p snap-test-pool bench --image rbd-test-0 --io-size 4M --io-total 400G --io-type write --io-pattern seq
    rbd snap create rbd-test-0@snap0 -p snap-test-pool

    # Write data,then purge snap
    rbd -p snap-test-pool bench --image rbd-test-0 --io-size 4M --io-total 400G --io-type write --io-pattern seq
    rbd snap rm rbd-test-0@snap0 -p snap-test-pool

    # Collect the results from the osd log
    mkdir -p ./test
    for num in {0..9};do
        cat ./out/osd.${num}.log | grep l_osd_snap_trim_get_raw_object_lat >> ./test/result.log
    done
}