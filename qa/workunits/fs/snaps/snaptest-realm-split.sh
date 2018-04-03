#!/bin/sh -x

set -e

ceph fs set cephfs allow_new_snaps true --yes-i-really-mean-it

mkdir -p 1/a
exec 3<> 1/a/file1

echo -n a >&3

mkdir 1/.snap/s1

echo -n b >&3

mkdir 2
# create new snaprealm at dir a, file1's cap should be attached to the new snaprealm
mv 1/a 2

mkdir 2/.snap/s2

echo -n c >&3

exec 3>&-

grep '^a$' 1/.snap/s1/a/file1
grep '^ab$' 2/.snap/s2/a/file1
grep '^abc$' 2/a/file1

rmdir 1/.snap/s1
rmdir 2/.snap/s2
rm -rf 1 2
echo OK
