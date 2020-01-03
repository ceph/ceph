#!/bin/sh -x

set -e

# To skirt around GPL compatibility issues:
wget http://download.ceph.com/qa/fsync-tester.c
gcc fsync-tester.c -o fsync-tester

./fsync-tester

echo $PATH
whereis lsof
lsof
