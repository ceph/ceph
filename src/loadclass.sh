#!/usr/bin/env bash

fname=$1
[ -z "$fname" ] && exit

[ -e $fname ] || { echo "file no found: $fname"; exit; }

name="`nm $fname | grep __cls_name__ | sed 's/.*__cls_name__//g' | head -1`"
[ -z "$name" ] && exit

ver="`nm $fname | grep __cls_ver__ | sed 's/.*__cls_ver__//g' | sed 's/_/\./g' | head -1`"
[ -z "$ver" ] && exit

echo loading $name v$ver
fl=`file $fname`

arch=""

[ `echo "$fl" | grep -c i386` -gt 0 ] && arch="i386"
[ `echo "$fl" | grep -c x86-64` -gt 0 ] && arch="x86-64"

[ -z "$arch" ] && { echo "lib architecture not identified"; exit; }

`dirname $0`/ceph class add $name $ver $arch --in-data=$fname


