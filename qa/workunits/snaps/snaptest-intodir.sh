#!/bin/sh -ex

ceph mds set allow_new_snaps true --yes-i-really-mean-it

# this tests fix for #1399
mkdir foo
mkdir foo/.snap/one
touch bar
mv bar foo
sync
# should not crash :)

mkdir baz
mkdir baz/.snap/two
mv baz foo
sync
# should not crash :)

# clean up.
rmdir foo/baz/.snap/two
rmdir foo/.snap/one
rm -r foo

echo OK