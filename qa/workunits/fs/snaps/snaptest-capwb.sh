#!/bin/sh -x

set -e

mkdir foo

ceph mds set allow_new_snaps true --yes-i-really-mean-it

# make sure mds handles it when the client does not send flushsnap
echo x > foo/x
sync
mkdir foo/.snap/ss
ln foo/x foo/xx
cat foo/.snap/ss/x
rmdir foo/.snap/ss

#
echo a > foo/a
echo b > foo/b
mkdir foo/.snap/s
r=`cat foo/.snap/s/a`
[ -z "$r" ] && echo "a appears empty in snapshot" && false

ln foo/b foo/b2
cat foo/.snap/s/b

echo "this used to hang:"
echo more >> foo/b2
echo "oh, it didn't hang! good job."
cat foo/b
rmdir foo/.snap/s

rm -r foo

echo OK