#!/bin/sh -x

set -e

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
