#!/bin/sh -ex

# dir: srcdn=destdn
mkdir ./a/dir1
mv ./a/dir1 ./a/dir1.renamed

# dir: diff
mkdir ./a/dir2
mv ./a/dir2 ./b/dir2

# dir: diff, child subtree on target
mkdir -p ./a/dir3/child/foo
$CEPH_TOOL mds tell 0 export_dir /a/dir3/child 1
sleep 5
mv ./a/dir3 ./b/dir3

# dir: diff, child subtree on other
mkdir -p ./a/dir4/child/foo
$CEPH_TOOL mds tell 0 export_dir /a/dir4/child 2
sleep 5
mv ./a/dir4 ./b/dir4

# dir: witness subtree adjustment
mkdir -p ./a/dir5/1/2/3/4
$CEPH_TOOL mds tell 0 export_dir /a/dir5/1/2/3 2
sleep 5
mv ./a/dir5 ./b

