#!/usr/bin/env bash

set -e

rgw_frontend=${RGW_FRONTEND:-"beast"}
script_root=`dirname $0`
script_root=`(cd $script_root;pwd)`
if [ -e CMakeCache.txt ]; then
    script_root=$PWD
elif [ -e $script_root/../build/CMakeCache.txt ]; then
    cd $script_root/../build
    script_root=$PWD
fi
ceph_bin=$script_root/bin
vstart_path=`dirname $0`

[ "$#" -lt 2 ] && echo "usage: $0 <name> <port> [params...]" && exit 1

name=$1
port=$2

shift 2

run_root=$script_root/run/$name
pidfile=$run_root/out/radosgw.${port}.pid
asokfile=$run_root/out/radosgw.${port}.asok
logfile=$run_root/out/radosgw.${port}.log

$vstart_path/mstop.sh $name radosgw $port

$vstart_path/mrun $name radosgw --rgw-frontends="$rgw_frontend port=$port" -n client.rgw --pid-file=$pidfile --admin-socket=$asokfile "$@" --log-file=$logfile
