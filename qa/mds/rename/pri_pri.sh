#!/bin/sh -ex

touch mnt/a/file1
touch mnt/a/file2
touch mnt/a/file3
touch mnt/b/file4

# srcdn=destdn
mv mnt/a/file1 mnt/a/file2

# different (srcdn != destdn)
mv mnt/a/file3 mnt/b/file4

