#!/bin/bash
set -e

echo "getting iogen"
wget http://ceph.com/qa/iogen_3.1p0.tar
tar -xvzf iogen_3.1p0.tar
cd iogen*
echo "making iogen"
make
echo "running iogen"
./iogen -n 5 -s 2g

