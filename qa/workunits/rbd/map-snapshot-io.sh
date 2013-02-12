#!/bin/sh
set -e

# http://tracker.ceph.com/issues/3964

rbd create image -s 100
rbd map image
udevadm settle  # note: newer versions of rbd do this for you.
dd if=/dev/zero of=/dev/rbd/rbd/image oflag=direct count=10
rbd snap create image@s1
dd if=/dev/zero of=/dev/rbd/rbd/image oflag=direct count=10   # used to fail
rbd snap rm image@s1
dd if=/dev/zero of=/dev/rbd/rbd/image oflag=direct count=10

echo OK
