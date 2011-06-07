#!/bin/sh -ex

# srcdn=destdn
touch mnt/a/file1
mv mnt/a/file1 mnt/a/file1.renamed

# different
touch mnt/a/file2
mv mnt/a/file2 mnt/b

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

