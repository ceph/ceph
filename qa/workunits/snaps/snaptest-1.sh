#!/bin/bash -x

set -e

ceph mds set allow_new_snaps true --yes-i-really-mean-it

echo 1 > file1
echo 2 > file2
echo 3 > file3
[ -e file4 ] && rm file4
mkdir .snap/snap1
echo 4 > file4
now=`ls`
then=`ls .snap/snap1`
rmdir .snap/snap1
if [ "$now" = "$then" ]; then
    echo live and snap contents are identical?
    false
fi

# do it again
echo 1 > file1
echo 2 > file2
echo 3 > file3
mkdir .snap/snap1
echo 4 > file4
rmdir .snap/snap1

rm file?

echo OK