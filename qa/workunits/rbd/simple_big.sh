#!/bin/sh -ex

[ -d /sys/bus/rbd ] || sudo modprobe rbd

#rbd create foo --size 1000000
rbd create foo --size 100000    # ok, a bit smaller!
sudo rbd map foo
sudo dd if=/dev/zero of=/dev/rbd/rbd/foo bs=1M
sudo dd if=/dev/rbd/rbd/foo of=/dev/null bs=1M
sudo rbd unmap foo
rbd rm foo

echo OK

