#!/usr/bin/env bash

# This is a test for https://tracker.ceph.com/issues/41404, verifying that udev
# events are properly reaped while the image is being (un)mapped in the kernel.
# UDEV_BUF_SIZE is 1M (giving us a 2M socket receive buffer), but modprobe +
# modprobe -r generate ~28M worth of "block" events.

set -ex

rbd create --size 1 img

ceph osd pause
sudo rbd map img &
PID=$!
sudo modprobe scsi_debug max_luns=16 add_host=16 num_parts=1 num_tgts=16
sudo udevadm settle
sudo modprobe -r scsi_debug
[[ $(rbd showmapped | wc -l) -eq 0 ]]
ceph osd unpause
wait $PID
[[ $(rbd showmapped | wc -l) -eq 2 ]]
sudo rbd unmap img

echo OK
