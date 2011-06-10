#!/bin/sh -x

set -e

wget http://ceph.newdream.net/qa/linux-2.6.33.tar.bz2
mkdir foo
cp linux* foo
mkdir foo/.snap/barsnap
rm foo/linux*
diff -q foo/.snap/barsnap/linux* linux* && echo "passed: files are identical"
