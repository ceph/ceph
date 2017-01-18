#!/bin/bash

set -e

wget http://download.ceph.com/qa/pjd-fstest-20090130-RC-aclfixes.tgz
tar zxvf pjd*.tgz
cd pjd*
make clean
make
cd ..
mkdir tmp
cd tmp
# must be root!
sudo prove -r -v --exec 'bash -x' ../pjd*/tests
cd ..
rm -rf tmp pjd*

