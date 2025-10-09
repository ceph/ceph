#!/usr/bin/env bash

# Start/restart a radosgw instance on the given mstart.sh cluster.

set -e

rgw_frontend=${RGW_FRONTEND:-"beast"}
script_root=$(dirname "$0")
script_root=$(cd "$script_root" && pwd)
[ -z "$BUILD_DIR" ] && BUILD_DIR=build
if [ -e CMakeCache.txt ]; then
    script_root=$PWD
elif [ -e "$script_root"/../${BUILD_DIR}/CMakeCache.txt ]; then
    cd "$script_root"/../${BUILD_DIR}
    script_root=$PWD
fi
#ceph_bin=$script_root/bin
vstart_path=$(dirname "$0")

[ "$#" -lt 3 ] && echo "usage: $0 <name> <port> <ssl-port> [params...]" && exit 1

name=$1
port=$2
ssl_port=$3
cert_param=""
port_param="port=$port"

if [ "$ssl_port" -gt 0 ]; then
    cert_param="ssl_certificate=./cert.pem"
    if [ "$rgw_frontend" = "civetweb" ]; then
        port_param="port=${port} port=${ssl_port}s"
    else
        port_param="port=${port} ssl_port=${ssl_port}"
    fi
fi

if [ -n "$RGW_FRONTEND_THREADS" ]; then
    set_frontend_threads="num_threads=$RGW_FRONTEND_THREADS"
fi

shift 3

run_root=$script_root/run/$name
pidfile=$run_root/out/radosgw.${port}.pid
asokfile=$run_root/out/radosgw.${port}.asok
logfile=$run_root/out/radosgw.${port}.log

"$vstart_path"/mstop.sh "$name" radosgw "$port"

"$vstart_path"/mrun "$name" ceph -c "$run_root"/ceph.conf \
	-k "$run_root"/keyring auth get-or-create client.rgw."$port" mon \
	'allow rw' osd 'allow rwx' mgr 'allow rw' >> "$run_root"/keyring

"$vstart_path"/mrun "$name" radosgw --rgw-frontends="$rgw_frontend $port_param $set_frontend_threads $cert_param" \
	-n client.rgw."$port" --pid-file="$pidfile" \
	--admin-socket="$asokfile" "$@" --log-file="$logfile"
