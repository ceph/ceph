#!/bin/sh -x

set -e

mkdir foo
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
