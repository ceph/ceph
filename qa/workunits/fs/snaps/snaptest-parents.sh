#!/bin/sh

set -ex

echo "making directory tree and files"
mkdir -p 1/a/b/c/
echo "i'm file1" > 1/a/file1
echo "i'm file2" > 1/a/b/file2
echo "i'm file3" > 1/a/b/c/file3
echo "snapshotting"
mkdir 1/.snap/foosnap1
mkdir 2
echo "moving tree"
mv 1/a 2
echo "checking snapshot contains tree..."
dir1=`find 1/.snap/foosnap1 | wc -w`
dir2=`find 2/ | wc -w`
#diff $dir1 $dir2 && echo "Success!"
test $dir1==$dir2 && echo "Success!"
echo "adding folder and file to tree..."
mkdir 2/a/b/c/d
echo "i'm file 4!" > 2/a/b/c/d/file4
echo "snapshotting tree 2"
mkdir 2/.snap/barsnap2
echo "comparing snapshots"
dir1=`find 1/.snap/foosnap1/ -maxdepth 2 | wc -w`
dir2=`find 2/.snap/barsnap2/ -maxdepth 2 | wc -w`
#diff $dir1 $dir2 && echo "Success!"
test $dir1==$dir2 && echo "Success!"
echo "moving subtree to first folder"
mv 2/a/b/c 1
echo "comparing snapshots and new tree"
dir1=`find 1/ | wc -w`
dir2=`find 2/.snap/barsnap2/a/b/c | wc -w`
#diff $dir1 $dir2 && echo "Success!"
test $dir1==$dir2 && echo "Success!"
rmdir 1/.snap/*
rmdir 2/.snap/*
echo "OK"
