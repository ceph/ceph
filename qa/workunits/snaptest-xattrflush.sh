#!/bin/sh -x

set -e

mkdir a
mkdir a/b
mkdir a/b/c
# b now has As but not Ax
setfattr -n user.foo a/b
mkdir a/.snap/s
mkdir a/b/cc
# b now has been cowed on the server, but we still have dirty xattr caps
getfattr -n user.foo a/b          # there they are...
r=`getfattr -n user.foo a/.snap/s/b`  # should be there, too!
echo $r
[ -z "$r" ] && echo "didn't find user.foo xattr in snapped version" && false

# ok, clean up
rmdir a/.snap/s
rm -r a

