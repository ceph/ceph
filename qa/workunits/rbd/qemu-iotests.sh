#!/bin/sh -ex

# Run qemu-iotests against rbd. These are block-level tests that go
# through qemu but do not involve running a full vm. Note that these
# require the admin ceph user, as there's no way to pass the ceph user
# to qemu-iotests currently.

# This will only work with particular qemu versions, like 1.0. Later
# versions of qemu include qemu-iotests directly in the qemu
# repository.
codevers=`lsb_release -sc`
iotests=qemu-iotests
testlist='001 002 003 004 005 008 009 010 011 021 025'

# See if we need to use the iotests suites in qemu (newer version).
# Right now, trusty is the only version that uses this.
for chkcode in "trusty"
do
    if [ "$chkcode" = "$codevers" ]
    then
        iotests=qemu/tests/qemu-iotests
    fi
done

if [ "$iotests" = "qemu/tests/qemu-iotests" ]
then
    git clone git://repo.or.cz/qemu.git
    testlist=$testlist' 032 033 055 077'
else
    git clone git://ceph.com/git/qemu-iotests.git
fi

cd "$iotests"

mkdir bin
# qemu-iotests expects a binary called just 'qemu' to be available
ln -s `which qemu-system-x86_64` bin/qemu

# TEST_DIR is the pool for rbd
TEST_DIR=rbd PATH="$PATH:$PWD/bin" ./check -rbd $testlist

if [ "$iotests" = "qemu/tests/qemu-iotests" ]
then
    cd ../../..
else
    cd ..
fi

dname=`echo $iotests | cut -d "/" -f1`
rm -rf $dname

