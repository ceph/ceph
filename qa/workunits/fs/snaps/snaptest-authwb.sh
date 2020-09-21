#!/bin/sh -x

set -e

ceph fs set cephfs allow_new_snaps true --yes-i-really-mean-it

touch foo
chmod +x foo
mkdir .snap/s
find .snap/s/foo -executable | grep foo
rmdir .snap/s
rm foo

echo OK