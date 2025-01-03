#!/usr/bin/env bash

set -ex

wget http://download.ceph.com/qa/wac.c
gcc -o wac wac.c

rbd create --size 300 img
DEV=$(sudo rbd map img)

sudo mkfs.ext4 $DEV
sudo mount $DEV /mnt
set +e
sudo timeout 5m ./wac -l 65536 -n 64 -r /mnt/wac-test
RET=$?
set -e
[[ $RET -eq 124 ]]
sudo killall -w wac || true  # wac forks
sudo umount /mnt

sudo wipefs -a $DEV
sudo vgcreate vg_img $DEV
sudo lvcreate -L 256M -n lv_img vg_img
udevadm settle
sudo mkfs.ext4 /dev/mapper/vg_img-lv_img
sudo mount /dev/mapper/vg_img-lv_img /mnt
set +e
sudo timeout 5m ./wac -l 65536 -n 64 -r /mnt/wac-test
RET=$?
set -e
[[ $RET -eq 124 ]]
sudo killall -w wac || true  # wac forks
sudo umount /mnt
sudo vgremove -f vg_img
sudo pvremove $DEV

sudo rbd unmap $DEV
rbd rm img

echo OK
