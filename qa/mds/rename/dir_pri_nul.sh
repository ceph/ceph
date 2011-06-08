#!/bin/sh -ex

# dir: srcdn=destdn
mkdir mnt/a/dir1
mv mnt/a/dir1 mnt/a/dir1.renamed

# dir: diff
mkdir mnt/a/dir2
mv mnt/a/dir2 mnt/b/dir2

# dir: diff, child subtree on target
mkdir -p mnt/a/dir3/child/foo
./ceph mds tell 0 export_dir /a/dir3/child 1
sleep 5
mv mnt/a/dir3 mnt/b/dir3

# dir: diff, child subtree on other
mkdir -p mnt/a/dir4/child/foo
./ceph mds tell 0 export_dir /a/dir4/child 2
sleep 5
mv mnt/a/dir4 mnt/b/dir4

# dir: witness subtree adjustment
mkdir -p mnt/a/dir5/1/2/3/4
./ceph mds tell 0 export_dir /a/dir5/1/2/3 2
sleep 5
mv mnt/a/dir5 mnt/b

