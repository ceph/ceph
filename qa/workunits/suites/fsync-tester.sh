#!/bin/sh

set -ex

# To skirt around GPL compatibility issues:
wget http://download.ceph.com/qa/fsync-tester.c
gcc -D_GNU_SOURCE fsync-tester.c -o fsync-tester

./fsync-tester

echo $PATH
whereis lsof
lsof
