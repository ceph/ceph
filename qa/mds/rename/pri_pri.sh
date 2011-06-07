#!/bin/sh -ex

# srcdn=destdn
touch mnt/a/file1
touch mnt/a/file2
mv mnt/a/file1 mnt/a/file2

# different (srcdn != destdn)
touch mnt/a/file3
touch mnt/b/file4
mv mnt/a/file3 mnt/b/file4

# dir, srcdn=destdn
mkdir mnt/a/dir1
mkdir mnt/a/dir2
mv -T mnt/a/dir1 mnt/a/dir2

# dir, different
mkdir mnt/a/dir3
mkdir mnt/b/dir4
mv -T mnt/a/dir3 mnt/b/dir4
