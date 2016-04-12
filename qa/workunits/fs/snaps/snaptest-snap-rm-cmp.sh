#!/bin/sh -x

set -e

ceph mds set allow_new_snaps true --yes-i-really-mean-it

file=linux-2.6.33.tar.bz2
wget -q http://download.ceph.com/qa/$file

real=`md5sum $file | awk '{print $1}'`

for f in `seq 1 20`
do
    echo $f
    cp $file a
    mkdir .snap/s
    rm a
    cp .snap/s/a /tmp/a
    cur=`md5sum /tmp/a | awk '{print $1}'`
    if [ "$cur" != "$real" ]; then
	echo "FAIL: bad match, /tmp/a $cur != real $real"
	false
    fi
    rmdir .snap/s
done
rm $file
