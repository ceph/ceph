#!/bin/bash

set -e

script_root=`dirname $0`

[ "$#" -lt 2 ] && echo "usage: $0 <name> <port> [params...]" && exit 1

name=$1
port=$2

shift 2

run_root=run/$name
pidfile=$run_root/out/radosgw.${port}.pid
asokfile=$run_root/out/radosgw.${port}.asok
logfile=$run_root/out/radosgw.${port}.log

$script_root/mstop.sh $name radosgw $port

$script_root/mrun $name radosgw --rgw-frontends="civetweb port=$port" --pid-file=$pidfile --admin-socket=$asokfile "$@" --log-file=$logfile
