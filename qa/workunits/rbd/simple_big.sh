#!/bin/sh -ex

mb=100000

rbd create foo --size $mb
DEV=$(sudo rbd map foo)
dd if=/dev/zero of=$DEV bs=1M count=$mb
dd if=$DEV of=/dev/null bs=1M count=$mb
sudo rbd unmap $DEV
rbd rm foo

echo OK
