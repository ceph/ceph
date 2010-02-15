#!/bin/bash

echo 1 > file1
echo 2 > file2
echo 3 > file3
mkdir .snap/snap1
echo 4 > file4
if [ `ls` -eq `ls .snap/snap1` ]; then
    echo live and snap contents are identical?
    false
fi