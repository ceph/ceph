#!/usr/bin/env bash

set -e

echo "getting dbench"
git clone git://git.samba.org/sahlberg/dbench.git dbench
cd dbench
./autogen.sh
./configure
make
make install
cp loadfiles/client.txt /usr/local/share/

echo "running dbench"
dbench 1
dbench 10

echo "deleting dbench file"
cd .. 
rm -rf dbench
rm -f /usr/local/share/client.txt 
