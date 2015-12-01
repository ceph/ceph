#!/bin/bash

set -e

mydir=`dirname $0`

wget http://download.ceph.com/qa/ffsb.tar.bz2
tar jxvf ffsb.tar.bz2
cd ffsb-*
./configure
make
cd ..
mkdir tmp
cd tmp

for f in $mydir/*.ffsb
do
    ../ffsb-*/ffsb $f
done
cd ..
rm -r tmp ffsb*

