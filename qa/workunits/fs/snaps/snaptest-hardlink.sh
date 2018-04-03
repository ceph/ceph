#!/bin/sh -x

set -e

ceph fs set cephfs allow_new_snaps true --yes-i-really-mean-it

mkdir 1 2
echo asdf >1/file1
echo asdf >1/file2

ln 1/file1 2/file1
ln 1/file2 2/file2

mkdir 2/.snap/s1

echo qwer >1/file1
grep asdf 2/.snap/s1/file1

rm -f 1/file2
grep asdf 2/.snap/s1/file2
rm -f 2/file2
grep asdf 2/.snap/s1/file2

rmdir 2/.snap/s1
rm -rf 1 2

echo OK
