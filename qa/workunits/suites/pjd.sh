#!/bin/bash

set -e

wget http://ceph.com/qa/pjd.tgz
tar zxvf pjd*.tgz
cd pjd*
make
cd ..
mkdir tmp
cd tmp
# must be root!
sudo prove -r -v ../pjd*/tests
cd ..
rm -r tmp pjd*

