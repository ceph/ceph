#!/bin/bash
wget http://ceph.newdream.net/qa/pjd-fstest-20080816.tgz
tar zxvf pjd*
cd pjd*
make
cd ..
mkdir tmp
cd tmp
prove -r ../pjd*/tests
