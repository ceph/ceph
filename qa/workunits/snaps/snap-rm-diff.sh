#!/bin/sh -ex

ceph mds set allow_new_snaps true --yes-i-really-mean-it
wget -q http://download.ceph.com/qa/linux-2.6.33.tar.bz2
mkdir foo
cp linux* foo
mkdir foo/.snap/barsnap
rm foo/linux*
diff -q foo/.snap/barsnap/linux* linux* && echo "passed: files are identical"
rmdir foo/.snap/barsnap
echo OK
