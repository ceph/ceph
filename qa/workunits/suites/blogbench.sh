#!/usr/bin/env bash
set -ex

echo "getting blogbench"
wget http://download.ceph.com/qa/blogbench-1.0.tar.bz2
#cp /home/gregf/src/blogbench-1.0.tar.bz2 .
tar -xvf blogbench-1.0.tar.bz2
cd blogbench-1.0/
echo "making blogbench"
export CFLAGS="-Wno-error=implicit-function-declaration"
./configure
make
cd src
mkdir blogtest_in
echo "running blogbench"
./blogbench -d blogtest_in
