#!/bin/sh -ex

# dir, srcdn=destdn
mkdir mnt/a/dir1
mkdir mnt/a/dir2
mv -T mnt/a/dir1 mnt/a/dir2

# dir, different
mkdir mnt/a/dir3
mkdir mnt/b/dir4
mv -T mnt/a/dir3 mnt/b/dir4
