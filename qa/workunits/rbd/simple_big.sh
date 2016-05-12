#!/bin/sh -ex

mb=100000

rbd create foo --size $mb
sudo rbd map foo
sudo dd if=/dev/zero of=/dev/rbd/rbd/foo bs=1M count=$mb
sudo dd if=/dev/rbd/rbd/foo of=/dev/null bs=1M count=$mb
sudo rbd unmap /dev/rbd/rbd/foo
rbd rm foo

echo OK

