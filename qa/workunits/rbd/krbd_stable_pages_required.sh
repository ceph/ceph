#!/usr/bin/env bash

set -ex

IMAGE_NAME="stable-pages-required-test"

rbd create --size 1 $IMAGE_NAME
DEV=$(sudo rbd map $IMAGE_NAME)
[[ $(blockdev --getsize64 $DEV) -eq 1048576 ]]
grep -q 1 /sys/block/${DEV#/dev/}/bdi/stable_pages_required

rbd resize --size 2 $IMAGE_NAME
[[ $(blockdev --getsize64 $DEV) -eq 2097152 ]]
grep -q 1 /sys/block/${DEV#/dev/}/bdi/stable_pages_required
sudo rbd unmap $DEV

echo OK
