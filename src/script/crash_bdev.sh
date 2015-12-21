#!/bin/bash -ex

while true; do
    ./ceph daemon osd.0 config set bdev_inject_crash 2
    sleep 5
    tail -n 1000 out/osd.0.log | grep bdev_inject_crash || exit 1
    ./init-ceph start osd.0
    sleep 20
done
