#!/bin/sh

# http://tracker.ceph.com/issues/3964

set -ex

rbd create image -s 100
DEV=$(sudo rbd map image)
dd if=/dev/zero of=$DEV oflag=direct count=10
rbd snap create image@s1
dd if=/dev/zero of=$DEV oflag=direct count=10   # used to fail
rbd snap rm image@s1
dd if=/dev/zero of=$DEV oflag=direct count=10
sudo rbd unmap $DEV
rbd rm image

echo OK
