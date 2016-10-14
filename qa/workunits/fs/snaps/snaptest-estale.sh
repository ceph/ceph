#!/bin/sh -x

ceph mds set allow_new_snaps true --yes-i-really-mean-it

mkdir .snap/foo

echo "We want ENOENT, not ESTALE, here."
for f in `seq 1 100`
do
    stat .snap/foo/$f 2>&1 | grep 'No such file'
done

rmdir .snap/foo

echo "OK"
