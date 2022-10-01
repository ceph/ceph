#!/bin/sh -ex

# srcdn=destdn
touch ./a/file1
mv ./a/file1 ./a/file1.renamed

# different
touch ./a/file2
mv ./a/file2 ./b


