#!/usr/bin/env bash
set -ex

echo "getting iogen"
wget http://download.ceph.com/qa/iogen_3.1p0.tar
tar -xvzf iogen_3.1p0.tar
cd iogen_3.1p0
echo "patching Makefile for distro compatibility"
# This removes the conflicting -Dstrlcpy and -Darc4random macros
sed -i 's/-Dstrlcpy=strncpy//g' Makefile
sed -i 's/-Darc4random=rand//g' Makefile

echo "injecting clean wrappers into src/iogen.c"
# Add standard definitions at the top of the C file to handle the missing BSD functions safely
sed -i '1s/^/#include <stdlib.h>\n#include <string.h>\n#define arc4random() (rand())\n#define strlcpy(dst,src,sz) (strncpy(dst,src,sz), (dst)[(sz)-1]="\\0")\n/' src/iogen.c

echo "making iogen"
make
echo "running iogen"
./iogen -n 5 -s 2g
echo "sleep for 10 min"
sleep 600
echo "stopping iogen"
./iogen -k

echo "OK"
