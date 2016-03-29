#!/bin/sh -x

set -e

ceph mds set allow_new_snaps true --yes-i-really-mean-it

#
# verify that a snap update on a parent realm will induce
# snap cap writeback for inodes child realms
#

mkdir a
mkdir a/b
mkdir a/.snap/a1
mkdir a/b/.snap/b1
echo asdf > a/b/foo
mkdir a/.snap/a2
# client _should_ have just queued a capsnap for writeback
ln a/b/foo a/b/bar       # make the server cow the inode

echo "this should not hang..."
cat a/b/.snap/_a2_*/foo
echo "good, it did not hang."

rmdir a/b/.snap/b1
rmdir a/.snap/a1
rmdir a/.snap/a2
rm -r a

echo "OK"