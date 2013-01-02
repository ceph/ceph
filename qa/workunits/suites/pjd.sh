#!/bin/bash

set -e

wget http://ceph.com/qa/pjd-fstest-20090130-RC-open24.tgz
tar zxvf pjd*.tgz
cd pjd*
make
cd ..
mkdir tmp
cd tmp
# must be root!
sudo prove -r -v --exec 'bash -x' ../pjd*/tests
cd ..
rm -rf tmp pjd*

