#!/bin/bash

set -e

mydir=`dirname $0`

wget http://ceph.com/qa/ffsb.tar.bz2
tar jxvf ffsb.tar.bz2
cd ffsb-*
wget -O config.guess "http://git.savannah.gnu.org/gitweb/?p=config.git;a=blob_plain;f=config.guess;hb=HEAD"
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

