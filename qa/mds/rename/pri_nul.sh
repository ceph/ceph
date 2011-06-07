#!/bin/sh -ex

touch mnt/a/file1
touch mnt/a/file2

# srcdn=destdn
mv mnt/a/file1 mnt/a/file1.renamed

# different
mv mnt/a/file2 mnt/b

