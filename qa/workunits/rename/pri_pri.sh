#!/bin/sh -ex

# srcdn=destdn
touch ./a/file1
touch ./a/file2
mv ./a/file1 ./a/file2

# different (srcdn != destdn)
touch ./a/file3
touch ./b/file4
mv ./a/file3 ./b/file4

