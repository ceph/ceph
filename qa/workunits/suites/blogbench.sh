#!/usr/bin/env bash
set -ex

echo "getting blogbench"
wget http://download.ceph.com/qa/blogbench-1.0.tar.bz2
#cp /home/gregf/src/blogbench-1.0.tar.bz2 .
tar -xvf blogbench-1.0.tar.bz2
cd blogbench-1.0/
echo "making blogbench"
./configure
make
cd src
mkdir blogtest_in
echo "running blogbench"
./blogbench -d blogtest_in
