#!/bin/bash

set -e

wget http://ceph.newdream.net/qa/pjd.tgz
tar zxvf pjd*.tgz
cd pjd*
make
cd ..
mkdir tmp
cd tmp
prove -r ../pjd*/tests
cd ..
rm -r tmp pjd*

