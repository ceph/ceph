#!/bin/sh -x

set -e

ceph mds set allow_new_snaps true --yes-i-really-mean-it

echo "testing simple xattr wb"
touch x
setfattr -n user.foo x
mkdir .snap/s1
getfattr -n user.foo .snap/s1/x | grep user.foo
rm x
rmdir .snap/s1

echo "testing wb with pre-wb server cow"
mkdir a
mkdir a/b
mkdir a/b/c
# b now has As but not Ax
setfattr -n user.foo a/b
mkdir a/.snap/s
mkdir a/b/cc
# b now has been cowed on the server, but we still have dirty xattr caps
getfattr -n user.foo a/b          # there they are...
getfattr -n user.foo a/.snap/s/b | grep user.foo  # should be there, too!

# ok, clean up
rmdir a/.snap/s
rm -r a

echo OK