#!/bin/sh -ex

# dir, srcdn=destdn
mkdir ./a/dir1
mkdir ./a/dir2
mv -T ./a/dir1 ./a/dir2

# dir, different
mkdir ./a/dir3
mkdir ./b/dir4
mv -T ./a/dir3 ./b/dir4
