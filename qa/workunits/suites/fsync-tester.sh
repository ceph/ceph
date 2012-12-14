#!/bin/sh -x

set -e

wget http://ceph.com/qa/fsync-tester.c
gcc fsync-tester.c -o fsync-tester

./fsync-tester
