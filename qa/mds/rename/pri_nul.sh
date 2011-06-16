#!/bin/sh -ex

# srcdn=destdn
touch mnt/a/file1
mv mnt/a/file1 mnt/a/file1.renamed

# different
touch mnt/a/file2
mv mnt/a/file2 mnt/b


