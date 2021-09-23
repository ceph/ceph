#!/bin/bash

set -x

# This script works best outside docker right now.

# TODO: remove this file in the future or extend with something more extensible.
# For now let's just use this.

# look for an available loop device
avail_loop=$(sudo losetup -f)
loop_name=$(basename -- $avail_loop)

# in case we have to create the loop, find the minor device number.
num_loops=$(lsmod | grep loop | awk '{print $3}')
num_loops=$((num_loops + 1))
echo creating loop $avail_loop minor: $num_loops
mknod $avail_loop b 7 $num_loops
sudo umount $avail_loop
sudo losetup -d $avail_loop
mkdir -p loop-images
sudo fallocate -l 10G "loop-images/disk${loop_name}.img"
sudo losetup $avail_loop "loop-images/disk${loop_name}.img"
sudo wipefs -a $avail_loop


# TODO: We will need more than one LVs
sudo lvm lvremove /dev/vg1/lv1
sudo lvm vgremove vg1
sudo pvcreate $avail_loop
sudo vgcreate vg1 $avail_loop
# 6G is arbitrary, osds need 5 I think. Just in case.
sudo lvcreate --size 6G --name lv1 vg1
