#!/bin/sh -x

set -e

ceph mds set allow_new_snaps true --yes-i-really-mean-it

#
# make sure we keep an existing dn's seq
#

mkdir a
mkdir .snap/bar
mkdir a/.snap/foo
rmdir a/.snap/foo
rmdir a
stat .snap/bar/a
rmdir .snap/bar

echo OK