#!/bin/sh -x

set -e

ceph mds set allow_new_snaps true --yes-i-really-mean-it

git clone git://git.ceph.com/ceph.git
cd ceph

versions=`seq 1 21`

for v in $versions
do
    ver="v0.$v"
    echo $ver
    git reset --hard $ver
    mkdir .snap/$ver
done

for v in $versions
do
    ver="v0.$v"
    echo checking $ver
    cd .snap/$ver
    git diff --exit-code
    cd ../..
done

for v in $versions
do
    ver="v0.$v"
    rmdir .snap/$ver
done

echo OK
