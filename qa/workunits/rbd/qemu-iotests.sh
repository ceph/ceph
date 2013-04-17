#!/bin/sh -ex

# Run qemu-iotests against rbd. These are block-level tests that go
# through qemu but do not involve running a full vm. Note that these
# require the admin ceph user, as there's no way to pass the ceph user
# to qemu-iotests currently.

# This will only work with particular qemu versions, like 1.0. Later
# versions of qemu includ qemu-iotests directly in the qemu
# repository.
git clone git://ceph.com/git/qemu-iotests.git

cd qemu-iotests
mkdir bin
# qemu-iotests expects a binary called just 'qemu' to be available
ln -s `which qemu-system-x86_64` bin/qemu

# TEST_DIR is the pool for rbd
TEST_DIR=rbd PATH="$PATH:$PWD/bin" ./check -rbd

cd ..
rm -rf qemu-iotests
