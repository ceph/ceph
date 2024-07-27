#!/usr/bin/env bash
# -*- mode:sh; tab-width:4; sh-basic-offset:4; indent-tabs-mode:nil -*-
# vim: softtabstop=4 shiftwidth=4 expandtab

# abort on failure
set -e

# flush local redis server
pid=`ps -ef | grep "redis-server"  | grep -v grep | awk -F' ' '{print $2}'`
if [[ -n $pid ]]; then
    echo "Flushing redis-server."
    redis-cli FLUSHALL
fi

quoted_print() {
    for s in "$@"; do
        if [[ "$s" =~ \  ]]; then
            printf -- "'%s' " "$s"
        else
            printf -- "$s "
        fi
    done
    printf '\n'
}

debug() {
  "$@" >&2
}

prunb() {
    debug quoted_print "$@" '&'
    PATH=$CEPH_BIN:$PATH "$@" &
}

prun() {
    debug quoted_print "$@"
    PATH=$CEPH_BIN:$PATH "$@"
}


if [ -n "$VSTART_DEST" ]; then
    SRC_PATH=`dirname $0`
    SRC_PATH=`(cd $SRC_PATH; pwd)`

    CEPH_DIR=$SRC_PATH
    CEPH_BIN=${CEPH_BIN:-${PWD}/bin}
    CEPH_LIB=${CEPH_LIB:-${PWD}/lib}

    CEPH_CONF_PATH=$VSTART_DEST
    CEPH_DEV_DIR=$VSTART_DEST/dev
    CEPH_OUT_DIR=$VSTART_DEST/out
    CEPH_ASOK_DIR=$VSTART_DEST/asok
    CEPH_OUT_CLIENT_DIR=${CEPH_OUT_CLIENT_DIR:-$CEPH_OUT_DIR}
fi

get_cmake_variable() {
    local variable=$1
    grep "${variable}:" CMakeCache.txt | cut -d "=" -f 2
}

# for running out of the CMake build directory
if [ -e CMakeCache.txt ]; then
    # Out of tree build, learn source location from CMakeCache.txt
    CEPH_ROOT=$(get_cmake_variable ceph_SOURCE_DIR)
    CEPH_BUILD_DIR=`pwd`
    [ -z "$MGR_PYTHON_PATH" ] && MGR_PYTHON_PATH=$CEPH_ROOT/src/pybind/mgr
fi

# use CEPH_BUILD_ROOT to vstart from a 'make install'
if [ -n "$CEPH_BUILD_ROOT" ]; then
    [ -z "$CEPH_BIN" ] && CEPH_BIN=$CEPH_BUILD_ROOT/bin
    [ -z "$CEPH_LIB" ] && CEPH_LIB=$CEPH_BUILD_ROOT/lib
    [ -z "$CEPH_EXT_LIB" ] && CEPH_EXT_LIB=$CEPH_BUILD_ROOT/external/lib
    [ -z "$EC_PATH" ] && EC_PATH=$CEPH_LIB/erasure-code
    [ -z "$OBJCLASS_PATH" ] && OBJCLASS_PATH=$CEPH_LIB/rados-classes
    # make install should install python extensions into PYTHONPATH
elif [ -n "$CEPH_ROOT" ]; then
    [ -z "$CEPHFS_SHELL" ] && CEPHFS_SHELL=$CEPH_ROOT/src/tools/cephfs/shell/cephfs-shell
    [ -z "$PYBIND" ] && PYBIND=$CEPH_ROOT/src/pybind
    [ -z "$CEPH_BIN" ] && CEPH_BIN=$CEPH_BUILD_DIR/bin
    [ -z "$CEPH_ADM" ] && CEPH_ADM=$CEPH_BIN/ceph
    [ -z "$INIT_CEPH" ] && INIT_CEPH=$CEPH_BIN/init-ceph
    [ -z "$CEPH_LIB" ] && CEPH_LIB=$CEPH_BUILD_DIR/lib
    [ -z "$CEPH_EXT_LIB" ] && CEPH_EXT_LIB=$CEPH_BUILD_DIR/external/lib
    [ -z "$OBJCLASS_PATH" ] && OBJCLASS_PATH=$CEPH_LIB
    [ -z "$EC_PATH" ] && EC_PATH=$CEPH_LIB
    [ -z "$CEPH_PYTHON_COMMON" ] && CEPH_PYTHON_COMMON=$CEPH_ROOT/src/python-common
fi

if [ -z "${CEPH_VSTART_WRAPPER}" ]; then
    PATH=$(pwd):$PATH
fi

[ -z "$PYBIND" ] && PYBIND=./pybind

[ -n "$CEPH_PYTHON_COMMON" ] && CEPH_PYTHON_COMMON="$CEPH_PYTHON_COMMON:"
CYTHON_PYTHONPATH="$CEPH_LIB/cython_modules/lib.3"
export PYTHONPATH=$PYBIND:$CYTHON_PYTHONPATH:$CEPH_PYTHON_COMMON$PYTHONPATH

export LD_LIBRARY_PATH=$CEPH_LIB:$CEPH_EXT_LIB:$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=$CEPH_LIB:$CEPH_EXT_LIB:$DYLD_LIBRARY_PATH
# Suppress logging for regular use that indicated that we are using a
# development version. vstart.sh is only used during testing and
# development
export CEPH_DEV=1

[ -z "$CEPH_NUM_MON" ] && CEPH_NUM_MON="$MON"
[ -z "$CEPH_NUM_OSD" ] && CEPH_NUM_OSD="$OSD"
[ -z "$CEPH_NUM_MDS" ] && CEPH_NUM_MDS="$MDS"
[ -z "$CEPH_NUM_MGR" ] && CEPH_NUM_MGR="$MGR"
[ -z "$CEPH_NUM_FS"  ] && CEPH_NUM_FS="$FS"
[ -z "$CEPH_NUM_RGW" ] && CEPH_NUM_RGW="$RGW"
[ -z "$GANESHA_DAEMON_NUM" ] && GANESHA_DAEMON_NUM="$NFS"

# if none of the CEPH_NUM_* number is specified, kill the existing
# cluster.
if [ -z "$CEPH_NUM_MON" -a \
     -z "$CEPH_NUM_OSD" -a \
     -z "$CEPH_NUM_MDS" -a \
     -z "$CEPH_NUM_MGR" -a \
     -z "$GANESHA_DAEMON_NUM" ]; then
    kill_all=1
else
    kill_all=0
fi

[ -z "$CEPH_NUM_MON" ] && CEPH_NUM_MON=3
[ -z "$CEPH_NUM_OSD" ] && CEPH_NUM_OSD=3
[ -z "$CEPH_NUM_MDS" ] && CEPH_NUM_MDS=3
[ -z "$CEPH_NUM_MGR" ] && CEPH_NUM_MGR=1
[ -z "$CEPH_NUM_FS"  ] && CEPH_NUM_FS=1
[ -z "$CEPH_MAX_MDS" ] && CEPH_MAX_MDS=1
[ -z "$CEPH_NUM_RGW" ] && CEPH_NUM_RGW=0
[ -z "$GANESHA_DAEMON_NUM" ] && GANESHA_DAEMON_NUM=0

[ -z "$CEPH_DIR" ] && CEPH_DIR="$PWD"
[ -z "$CEPH_DEV_DIR" ] && CEPH_DEV_DIR="$CEPH_DIR/dev"
[ -z "$CEPH_OUT_DIR" ] && CEPH_OUT_DIR="$CEPH_DIR/out"
[ -z "$CEPH_ASOK_DIR" ] && CEPH_ASOK_DIR="$CEPH_DIR/asok"
[ -z "$CEPH_RGW_PORT" ] && CEPH_RGW_PORT=8000
[ -z "$CEPH_CONF_PATH" ] && CEPH_CONF_PATH=$CEPH_DIR
CEPH_OUT_CLIENT_DIR=${CEPH_OUT_CLIENT_DIR:-$CEPH_OUT_DIR}

if [ $CEPH_NUM_OSD -gt 3 ]; then
    OSD_POOL_DEFAULT_SIZE=3
else
    OSD_POOL_DEFAULT_SIZE=$CEPH_NUM_OSD
fi

extra_conf=""
new=0
standby=0
debug=0
trace=0
ip=""
nodaemon=0
redirect=0
smallmds=0
short=0
crimson=0
ec=0
cephadm=0
parallel=true
restart=1
hitset=""
overwrite_conf=0
cephx=1 #turn cephx on by default
gssapi_authx=0
cache=""
if [ `uname` = FreeBSD ]; then
    objectstore="memstore"
else
    objectstore="bluestore"
fi
ceph_osd=ceph-osd
rgw_frontend="beast"
rgw_compression=""
rgw_store="rados"
lockdep=${LOCKDEP:-1}
spdk_enabled=0 # disable SPDK by default
pmem_enabled=0
io_uring_enabled=0
with_jaeger=0
force_addr=0
osds_per_host=0
require_osd_and_client_version=""
use_crush_tunables=""

with_mgr_dashboard=true
if [[ "$(get_cmake_variable WITH_MGR_DASHBOARD_FRONTEND)" != "ON" ]] ||
   [[ "$(get_cmake_variable WITH_RBD)" != "ON" ]]; then
    debug echo "ceph-mgr dashboard not built - disabling."
    with_mgr_dashboard=false
fi
with_mgr_restful=false

kstore_path=
declare -a block_devs
declare -a bluestore_db_devs
declare -a bluestore_wal_devs
declare -a secondary_block_devs
secondary_block_devs_type="SSD"

VSTART_SEC="client.vstart.sh"

MON_ADDR=""
DASH_URLS=""
RESTFUL_URLS=""

conf_fn="$CEPH_CONF_PATH/ceph.conf"
keyring_fn="$CEPH_CONF_PATH/keyring"
monmap_fn="/tmp/ceph_monmap.$$"
inc_osd_num=0

msgr="21"

read -r -d '' usage <<EOF || true
usage: $0 [option]... \nex: MON=3 OSD=1 MDS=1 MGR=1 RGW=1 NFS=1 NVMEOF_GW=ceph:5500 $0 -n -d
options:
	-d, --debug
	-t, --trace
	-s, --standby_mds: Generate standby-replay MDS for each active
	-l, --localhost: use localhost instead of hostname
	-i <ip>: bind to specific ip
	-n, --new
	--valgrind[_{osd,mds,mon,rgw}] 'toolname args...'
	--nodaemon: use ceph-run as wrapper for mon/osd/mds
	--redirect-output: only useful with nodaemon, directs output to log file
	--smallmds: limit mds cache memory limit
	-m ip:port		specify monitor address
	-k keep old configuration files (default)
	-x enable cephx (on by default)
	-X disable cephx
	-g --gssapi enable Kerberos/GSSApi authentication
	-G disable Kerberos/GSSApi authentication
	--hitset <pool> <hit_set_type>: enable hitset tracking
	-e : create an erasure pool
	-o config add extra config parameters to all sections
	--rgw_port specify ceph rgw http listen port
	--rgw_frontend specify the rgw frontend configuration
	--rgw_arrow_flight start arrow flight frontend
	--rgw_compression specify the rgw compression plugin
	--rgw_store storage backend: rados|dbstore|posix
	--seastore use seastore as crimson osd backend
	-b, --bluestore use bluestore as the osd objectstore backend (default)
	-K, --kstore use kstore as the osd objectstore backend
	--cyanstore use cyanstore as the osd objectstore backend
	--memstore use memstore as the osd objectstore backend
	--cache <pool>: enable cache tiering on pool
	--short: short object names only; necessary for ext4 dev
	--nolockdep disable lockdep
	--multimds <count> allow multimds with maximum active count
	--without-dashboard: do not run using mgr dashboard
	--bluestore-spdk: enable SPDK and with a comma-delimited list of PCI-IDs of NVME device (e.g, 0000:81:00.0)
	--bluestore-pmem: enable PMEM and with path to a file mapped to PMEM
	--msgr1: use msgr1 only
	--msgr2: use msgr2 only
	--msgr21: use msgr2 and msgr1
	--crimson: use crimson-osd instead of ceph-osd
	--crimson-foreground: use crimson-osd, but run it in the foreground
	--osd-args: specify any extra osd specific options
	--bluestore-devs: comma-separated list of blockdevs to use for bluestore
	--bluestore-db-devs: comma-separated list of db-devs to use for bluestore
	--bluestore-wal-devs: comma-separated list of wal-devs to use for bluestore
	--bluestore-io-uring: enable io_uring backend
	--inc-osd: append some more osds into existing vcluster
	--cephadm: enable cephadm orchestrator with ~/.ssh/id_rsa[.pub]
	--no-parallel: dont start all OSDs in parallel
	--no-restart: dont restart process when using ceph-run
	--jaeger: use jaegertracing for tracing
	--seastore-device-size: set total size of seastore
	--seastore-devs: comma-separated list of blockdevs to use for seastore
	--seastore-secondary-devs: comma-separated list of secondary blockdevs to use for seastore
	--seastore-secondary-devs-type: device type of all secondary blockdevs. HDD, SSD(default), ZNS or RANDOM_BLOCK_SSD
	--crimson-smp: number of cores to use for crimson
	--crimson-alien-num-threads: number of alien-tp threads
	--crimson-alien-num-cores: number of cores to use for alien-tp
	--osds-per-host: populate crush_location as each host holds the specified number of osds if set
	--require-osd-and-client-version: if supplied, do set-require-min-compat-client and require-osd-release to specified value
	--use-crush-tunables: if supplied, set tunables to specified value
\n
EOF

usage_exit() {
    printf "$usage"
    exit
}

parse_block_devs() {
    local opt_name=$1
    shift
    local devs=$1
    shift
    local dev
    IFS=',' read -r -a block_devs <<< "$devs"
    for dev in "${block_devs[@]}"; do
        if [ ! -b $dev ] || [ ! -w $dev ]; then
            echo "All $opt_name must refer to writable block devices"
            exit 1
        fi
    done
}

parse_bluestore_db_devs() {
    local opt_name=$1
    shift
    local devs=$1
    shift
    local dev
    IFS=',' read -r -a bluestore_db_devs <<< "$devs"
    for dev in "${bluestore_db_devs[@]}"; do
        if [ ! -b $dev ] || [ ! -w $dev ]; then
            echo "All $opt_name must refer to writable block devices"
            exit 1
        fi
    done
}

parse_bluestore_wal_devs() {
    local opt_name=$1
    shift
    local devs=$1
    shift
    local dev
    IFS=',' read -r -a bluestore_wal_devs <<< "$devs"
    for dev in "${bluestore_wal_devs[@]}"; do
        if [ ! -b $dev ] || [ ! -w $dev ]; then
            echo "All $opt_name must refer to writable block devices"
            exit 1
        fi
    done
}

parse_secondary_devs() {
    local opt_name=$1
    shift
    local devs=$1
    shift
    local dev
    IFS=',' read -r -a secondary_block_devs <<< "$devs"
    for dev in "${secondary_block_devs[@]}"; do
        if [ ! -b $dev ] || [ ! -w $dev ]; then
            echo "All $opt_name must refer to writable block devices"
            exit 1
        fi
    done
}

# Default values for the crimson options
crimson_smp=1
crimson_alien_num_threads=0
crimson_alien_num_cores=0

while [ $# -ge 1 ]; do
case $1 in
    -d | --debug)
        debug=1
        ;;
    -t | --trace)
        trace=1
        ;;
    -s | --standby_mds)
        standby=1
        ;;
    -l | --localhost)
        ip="127.0.0.1"
        force_addr=1
        ;;
    -i)
        [ -z "$2" ] && usage_exit
        ip="$2"
        shift
        ;;
    -e)
        ec=1
        ;;
    --new | -n)
        new=1
        ;;
    --inc-osd)
        new=0
        kill_all=0
        inc_osd_num=$2
        if [ "$inc_osd_num" == "" ]; then
            inc_osd_num=1
        else
            shift
        fi
        ;;
    --short)
        short=1
        ;;
    --crimson)
        crimson=1
        ceph_osd=crimson-osd
        nodaemon=1
        msgr=2
        ;;
    --crimson-foreground)
        crimson=1
        ceph_osd=crimson-osd
        nodaemon=0
        msgr=2
        ;;
    --osd-args)
        extra_osd_args="$2"
        shift
        ;;
    --msgr1)
        msgr="1"
        ;;
    --msgr2)
        msgr="2"
        ;;
    --msgr21)
        msgr="21"
        ;;
    --cephadm)
        cephadm=1
        ;;
    --no-parallel)
        parallel=false
        ;;
    --no-restart)
        restart=0
        ;;
    --valgrind)
        [ -z "$2" ] && usage_exit
        valgrind=$2
        shift
        ;;
    --valgrind_args)
        valgrind_args="$2"
        shift
        ;;
    --valgrind_mds)
        [ -z "$2" ] && usage_exit
        valgrind_mds=$2
        shift
        ;;
    --valgrind_osd)
        [ -z "$2" ] && usage_exit
        valgrind_osd=$2
        shift
        ;;
    --valgrind_mon)
        [ -z "$2" ] && usage_exit
        valgrind_mon=$2
        shift
        ;;
    --valgrind_mgr)
        [ -z "$2" ] && usage_exit
        valgrind_mgr=$2
        shift
        ;;
    --valgrind_rgw)
        [ -z "$2" ] && usage_exit
        valgrind_rgw=$2
        shift
        ;;
    --nodaemon)
        nodaemon=1
        ;;
    --redirect-output)
        redirect=1
        ;;
    --smallmds)
        smallmds=1
        ;;
    --rgw_port)
        CEPH_RGW_PORT=$2
        shift
        ;;
    --rgw_frontend)
        rgw_frontend=$2
        shift
        ;;
    --rgw_arrow_flight)
        rgw_flight_frontend="yes"
        ;;
    --rgw_compression)
        rgw_compression=$2
        shift
        ;;
    --rgw_store)
        rgw_store=$2
        shift
        ;;
    --kstore_path)
        kstore_path=$2
        shift
        ;;
    -m)
        [ -z "$2" ] && usage_exit
        MON_ADDR=$2
        shift
        ;;
    -x)
        cephx=1 # this is on be default, flag exists for historical consistency
        ;;
    -X)
        cephx=0
        ;;

    -g | --gssapi)
        gssapi_authx=1
        ;;
    -G)
        gssapi_authx=0
        ;;

    -k)
        if [ ! -r $conf_fn ]; then
            echo "cannot use old configuration: $conf_fn not readable." >&2
            exit
        fi
        new=0
        ;;
    --memstore)
        objectstore="memstore"
        ;;
    --cyanstore)
        objectstore="cyanstore"
        ;;
    --seastore)
        objectstore="seastore"
        ;;
    -b | --bluestore)
        objectstore="bluestore"
        ;;
    -K | --kstore)
        objectstore="kstore"
        ;;
    --hitset)
        hitset="$hitset $2 $3"
        shift
        shift
        ;;
    -o)
        extra_conf+=$'\n'"$2"
        shift
        ;;
    --cache)
        if [ -z "$cache" ]; then
            cache="$2"
        else
            cache="$cache $2"
        fi
        shift
        ;;
    --nolockdep)
        lockdep=0
        ;;
    --multimds)
        CEPH_MAX_MDS="$2"
        shift
        ;;
    --without-dashboard)
        with_mgr_dashboard=false
        ;;
    --with-restful)
        with_mgr_restful=true
        ;;
    --seastore-device-size)
        seastore_size="$2"
        shift
        ;;
    --seastore-devs)
        parse_block_devs --seastore-devs "$2"
        shift
        ;;
    --seastore-secondary-devs)
        parse_secondary_devs --seastore-devs "$2"
        shift
        ;;
    --seastore-secondary-devs-type)
        secondary_block_devs_type="$2"
        shift
        ;;
    --crimson-smp)
        crimson_smp=$2
        shift
        ;;
    --crimson-alien-num-threads)
        crimson_alien_num_threads=$2
        shift
        ;;
    --crimson-alien-num-cores)
        crimson_alien_num_cores=$2
        shift
        ;;
    --bluestore-spdk)
        [ -z "$2" ] && usage_exit
        IFS=',' read -r -a bluestore_spdk_dev <<< "$2"
        spdk_enabled=1
        shift
        ;;
    --bluestore-pmem)
        [ -z "$2" ] && usage_exit
        bluestore_pmem_file="$2"
        pmem_enabled=1
        shift
        ;;
    --bluestore-devs)
        parse_block_devs --bluestore-devs "$2"
        shift
        ;;
    --bluestore-db-devs)
        parse_bluestore_db_devs --bluestore-db-devs "$2"
        shift
        ;;
    --bluestore-wal-devs)
        parse_bluestore_wal_devs --bluestore-wal-devs "$2"
        shift
        ;;
    --bluestore-io-uring)
        io_uring_enabled=1
        shift
        ;;
    --jaeger)
        with_jaeger=1
        echo "with_jaeger $with_jaeger"
        ;;
    --osds-per-host)
        osds_per_host="$2"
        shift
        echo "osds_per_host $osds_per_host"
        ;;
    --require-osd-and-client-version)
        require_osd_and_client_version="$2"
        shift
        echo "require_osd_and_client_version $require_osd_and_client_version"
        ;;
    --use-crush-tunables)
        use_crush_tunables="$2"
        shift
        echo "use_crush_tunables $use_crush_tunables"
        ;;
    *)
        usage_exit
esac
shift
done

if [ $kill_all -eq 1 ]; then
    $SUDO $INIT_CEPH stop
fi

if [ "$new" -eq 0 ]; then
    if [ -z "$CEPH_ASOK_DIR" ]; then
        CEPH_ASOK_DIR=`dirname $($CEPH_BIN/ceph-conf  -c $conf_fn --show-config-value admin_socket)`
    fi
    mkdir -p $CEPH_ASOK_DIR
    MON=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_mon 2>/dev/null` && \
        CEPH_NUM_MON="$MON"
    OSD=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_osd 2>/dev/null` && \
        CEPH_NUM_OSD="$OSD"
    MDS=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_mds 2>/dev/null` && \
        CEPH_NUM_MDS="$MDS"
    MGR=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_mgr 2>/dev/null` && \
        CEPH_NUM_MGR="$MGR"
    RGW=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_rgw 2>/dev/null` && \
        CEPH_NUM_RGW="$RGW"
    NFS=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_ganesha 2>/dev/null` && \
        GANESHA_DAEMON_NUM="$NFS"
else
    # only delete if -n
    if [ -e "$conf_fn" ]; then
        asok_dir=`dirname $($CEPH_BIN/ceph-conf  -c $conf_fn --show-config-value admin_socket)`
        rm -- "$conf_fn"
        if [ $asok_dir != /var/run/ceph ]; then
            [ -d $asok_dir ] && rm -f $asok_dir/* && rmdir $asok_dir
        fi
    fi
    if [ -z "$CEPH_ASOK_DIR" ]; then
        CEPH_ASOK_DIR=`mktemp -u -d "${TMPDIR:-/tmp}/ceph-asok.XXXXXX"`
    fi
fi

ARGS="-c $conf_fn"

run() {
    type=$1
    shift
    num=$1
    shift
    eval "valg=\$valgrind_$type"
    [ -z "$valg" ] && valg="$valgrind"

    if [ -n "$valg" ]; then
        prunb valgrind --tool="$valg" $valgrind_args "$@" -f
        sleep 1
    else
        if [ "$nodaemon" -eq 0 ]; then
            prun "$@"
        else
            if [ "$restart" -eq 0 ]; then
                set -- '--no-restart' "$@"
            fi
            if [ "$redirect" -eq 0 ]; then
                prunb ${CEPH_ROOT}/src/ceph-run "$@" -f
            else
                ( prunb ${CEPH_ROOT}/src/ceph-run "$@" -f ) >$CEPH_OUT_DIR/$type.$num.stdout 2>&1
            fi
        fi
    fi
}

wconf() {
    if [ "$new" -eq 1 -o "$overwrite_conf" -eq 1 ]; then
        cat >> "$conf_fn"
    fi
}


do_rgw_conf() {

    if [ $CEPH_NUM_RGW -eq 0 ]; then
        return 0
    fi

    # setup each rgw on a sequential port, starting at $CEPH_RGW_PORT.
    # individual rgw's ids will be their ports.
    current_port=$CEPH_RGW_PORT
    # allow only first rgw to start arrow_flight server/port
    local flight_conf=$rgw_flight_frontend
    for n in $(seq 1 $CEPH_NUM_RGW); do
        wconf << EOF
[client.rgw.${current_port}]
        rgw frontends = $rgw_frontend port=${current_port}${flight_conf:+,arrow_flight}
        admin socket = ${CEPH_OUT_DIR}/radosgw.${current_port}.asok
        debug rgw_flight = 20
        debug rgw_notification = 20
EOF
        current_port=$((current_port + 1))
        unset flight_conf
done

}

do_rgw_dbstore_conf() {
    if [ $CEPH_NUM_RGW -gt 1 ]; then
        echo "dbstore is not distributed so only works with CEPH_NUM_RGW=1"
        exit 1
    fi

    prun mkdir -p "$CEPH_DEV_DIR/rgw/dbstore"
    wconf <<EOF
        rgw backend store = dbstore
        rgw config store = dbstore
        dbstore db dir = $CEPH_DEV_DIR/rgw/dbstore
        dbstore_config_uri = file://$CEPH_DEV_DIR/rgw/dbstore/config.db

EOF
}

format_conf() {
    local opts=$1
    local indent="        "
    local opt
    local formatted
    while read -r opt; do
        if [ -z "$formatted" ]; then
            formatted="${opt}"
        else
            formatted+=$'\n'${indent}${opt}
        fi
    done <<< "$opts"
    echo "$formatted"
}

prepare_conf() {
    local DAEMONOPTS="
        log file = $CEPH_OUT_DIR/\$name.log
        admin socket = $CEPH_ASOK_DIR/\$name.asok
        chdir = \"\"
        pid file = $CEPH_OUT_DIR/\$name.pid
        heartbeat file = $CEPH_OUT_DIR/\$name.heartbeat
"

    local mgr_modules="iostat nfs"
    if $with_mgr_dashboard; then
        mgr_modules+=" dashboard"
    fi
    if $with_mgr_restful; then
        mgr_modules+=" restful"
    fi

    local msgr_conf=''
    if [ $msgr -eq 21 ]; then
        msgr_conf="ms bind msgr2 = true
                   ms bind msgr1 = true"
    fi
    if [ $msgr -eq 2 ]; then
        msgr_conf="ms bind msgr2 = true
                   ms bind msgr1 = false"
    fi
    if [ $msgr -eq 1 ]; then
        msgr_conf="ms bind msgr2 = false
                   ms bind msgr1 = true"
    fi
    if [ $force_addr -eq 1 ]; then
        msgr_conf+="
                   public bind addr = $IP
                   public addr = $IP
                   cluster addr = $IP"
    fi

    wconf <<EOF
; generated by vstart.sh on `date`
[$VSTART_SEC]
        num mon = $CEPH_NUM_MON
        num osd = $CEPH_NUM_OSD
        num mds = $CEPH_NUM_MDS
        num mgr = $CEPH_NUM_MGR
        num rgw = $CEPH_NUM_RGW
        num ganesha = $GANESHA_DAEMON_NUM

[global]
        fsid = $(uuidgen)
        osd failsafe full ratio = .99
        mon osd full ratio = .99
        mon osd nearfull ratio = .99
        mon osd backfillfull ratio = .99
        mon_max_pg_per_osd = ${MON_MAX_PG_PER_OSD:-1000}
        erasure code dir = $EC_PATH
        plugin dir = $CEPH_LIB
        run dir = $CEPH_OUT_DIR
        crash dir = $CEPH_OUT_DIR
        enable experimental unrecoverable data corrupting features = *
        osd_crush_chooseleaf_type = 0
        debug asok assert abort = true
        $(format_conf "${msgr_conf}")
        $(format_conf "${extra_conf}")
        $AUTOSCALER_OPTS
EOF
    if [ "$with_jaeger" -eq 1 ] ; then
        wconf <<EOF
        jaeger_agent_port = 6831
EOF
    fi
    if [ "$lockdep" -eq 1 ] ; then
        wconf <<EOF
        lockdep = true
EOF
    fi
    if [ "$cephx" -eq 1 ] ; then
        wconf <<EOF
        auth cluster required = cephx
        auth service required = cephx
        auth client required = cephx
EOF
    elif [ "$gssapi_authx" -eq 1 ] ; then
        wconf <<EOF
        auth cluster required = gss
        auth service required = gss
        auth client required = gss
        gss ktab client file = $CEPH_DEV_DIR/gss_\$name.keytab
EOF
    else
        wconf <<EOF
        auth cluster required = none
        auth service required = none
        auth client required = none
        ms mon client mode = crc
EOF
    fi
    if [ "$short" -eq 1 ]; then
        COSDSHORT="        osd max object name len = 460
        osd max object namespace len = 64"
    fi
    if [ "$objectstore" == "bluestore" ]; then
        if [ "$spdk_enabled" -eq 1 ] || [ "$pmem_enabled" -eq 1 ]; then
            BLUESTORE_OPTS="        bluestore_block_db_path = \"\"
        bluestore_block_db_size = 0
        bluestore_block_db_create = false
        bluestore_block_wal_path = \"\"
        bluestore_block_wal_size = 0
        bluestore_block_wal_create = false
        bluestore_spdk_mem = 2048"
        else
            BLUESTORE_OPTS="        bluestore block db path = $CEPH_DEV_DIR/osd\$id/block.db.file
        bluestore block db size = 1073741824
        bluestore block db create = true
        bluestore block wal path = $CEPH_DEV_DIR/osd\$id/block.wal.file
        bluestore block wal size = 1048576000
        bluestore block wal create = true"
            if [ ${#block_devs[@]} -gt 0 ] || \
               [ ${#bluestore_db_devs[@]} -gt 0 ] || \
               [ ${#bluestore_wal_devs[@]} -gt 0 ]; then
                # when use physical disk, not create file for db/wal
                BLUESTORE_OPTS=""
            fi
        fi
        if [ "$io_uring_enabled" -eq 1 ]; then
            BLUESTORE_OPTS+="
        bdev ioring = true"
        fi
    fi

    if [ "$objectstore" == "seastore" ]; then
      if [[ ${seastore_size+x} ]]; then
        SEASTORE_OPTS="
        seastore device size = $seastore_size"
      fi
    fi

    wconf <<EOF
[client]
$CCLIENTDEBUG
        keyring = $keyring_fn
        log file = $CEPH_OUT_CLIENT_DIR/\$name.\$pid.log
        admin socket = $CEPH_ASOK_DIR/\$name.\$pid.asok

        ; needed for s3tests
        rgw crypt s3 kms backend = testing
        rgw crypt s3 kms encryption keys = testkey-1=YmluCmJvb3N0CmJvb3N0LWJ1aWxkCmNlcGguY29uZgo= testkey-2=aWIKTWFrZWZpbGUKbWFuCm91dApzcmMKVGVzdGluZwo=
        rgw crypt require ssl = false
        rgw sts key = abcdefghijklmnop
        rgw s3 auth use sts = true
        ; uncomment the following to set LC days as the value in seconds;
        ; needed for passing lc time based s3-tests (can be verbose)
        ; rgw lc debug interval = 10
        $(format_conf "${extra_conf}")
EOF
    if [ "$rgw_store" == "dbstore" ] ; then
        do_rgw_dbstore_conf
    elif [ "$rgw_store" == "posix" ] ; then
        # use dbstore as the backend and posix as the filter
        do_rgw_dbstore_conf
        posix_dir="$CEPH_DEV_DIR/rgw/posix"
        prun mkdir -p $posix_dir/root $posix_dir/lmdb
        wconf <<EOF
        rgw filter = posix
        rgw posix base path = $posix_dir/root
        rgw posix database root = $posix_dir/lmdb

EOF
    fi
	do_rgw_conf
	wconf << EOF
[mds]
$CMDSDEBUG
$DAEMONOPTS
        mds data = $CEPH_DEV_DIR/mds.\$id
        mds root ino uid = `id -u`
        mds root ino gid = `id -g`
        $(format_conf "${extra_conf}")
[mgr]
        mgr disabled modules = rook
        mgr data = $CEPH_DEV_DIR/mgr.\$id
        mgr module path = $MGR_PYTHON_PATH
        cephadm path = $CEPH_BIN/cephadm
$DAEMONOPTS
        $(format_conf "${extra_conf}")
[osd]
$DAEMONOPTS
        osd_check_max_object_name_len_on_startup = false
        osd data = $CEPH_DEV_DIR/osd\$id
        osd journal = $CEPH_DEV_DIR/osd\$id/journal
        osd journal size = 100
        osd class tmp = out
        osd class dir = $OBJCLASS_PATH
        osd class load list = *
        osd class default list = *
        osd fast shutdown = false

        bluestore fsck on mount = true
        bluestore block create = true
$BLUESTORE_OPTS

        ; kstore
        kstore fsck on mount = true
        osd objectstore = $objectstore
$SEASTORE_OPTS
$COSDSHORT
        $(format_conf "${extra_conf}")
[mon]
        mon_data_avail_crit = 1
        mgr initial modules = $mgr_modules
$DAEMONOPTS
$CMONDEBUG
        $(format_conf "${extra_conf}")
        mon cluster log file = $CEPH_OUT_DIR/cluster.mon.\$id.log
        osd pool default erasure code profile = plugin=jerasure technique=reed_sol_van k=2 m=1 crush-failure-domain=osd
        auth allow insecure global id reclaim = false
EOF

    if [ "$crimson" -eq 1 ]; then
        wconf <<EOF
        osd pool default crimson = true
EOF
    fi

    # this is most probably a bug in ceph_mon
    # but public_bind_addr set in [global] doesn't work
    # when mon_host is also provided, resulting
    # in different public and bind addresses (ports)
    # As a result, no client can connect to the mon.
    # The problematic code is in ceph_mon.cc, it looks like
    #
    #   // check if the public_bind_addr option is set
    #  if (!g_conf()->public_bind_addr.is_blank_ip()) {
    #    bind_addrs = make_mon_addrs(g_conf()->public_bind_addr);
    #  }
    #
    if [ $force_addr -eq 1 ]; then
        wconf <<EOF
        ; this is to counter the explicit public_bind_addr in the [global] section
        ; see src/vstart.sh for more info
        public bind addr =
EOF
    fi   
}

write_logrotate_conf() {
    out_dir=$(pwd)"/out/*.log"

    cat << EOF
$out_dir
{
    rotate 5
    size 1G
    copytruncate
    compress
    notifempty
    missingok
    sharedscripts
    postrotate
        # NOTE: assuring that the absence of one of the following processes
        # won't abort the logrotate command.
        killall -u $USER -q -1 ceph-mon ceph-mgr ceph-mds ceph-osd ceph-fuse radosgw rbd-mirror || echo ""
    endscript
}
EOF
}

init_logrotate() {
    logrotate_conf_path=$(pwd)"/logrotate.conf"
    logrotate_state_path=$(pwd)"/logrotate.state"

    if ! test -a $logrotate_conf_path; then
        if test -a $logrotate_state_path; then
            rm -f $logrotate_state_path
        fi
        write_logrotate_conf > $logrotate_conf_path
    fi
}

start_mon() {
    local MONS=""
    local count=0
    for f in a b c d e f g h i j k l m n o p q r s t u v w x y z
    do
        [ $count -eq $CEPH_NUM_MON ] && break;
        count=$(($count + 1))
        if [ -z "$MONS" ]; then
	    MONS="$f"
        else
	    MONS="$MONS $f"
        fi
    done

    if [ "$new" -eq 1 ]; then
        if [ `echo $IP | grep '^127\\.'` ]; then
            echo
            echo "NOTE: hostname resolves to loopback; remote hosts will not be able to"
            echo "  connect.  either adjust /etc/hosts, or edit this script to use your"
            echo "  machine's real IP."
            echo
        fi

        prun $SUDO "$CEPH_BIN/ceph-authtool" --create-keyring --gen-key --name=mon. "$keyring_fn" --cap mon 'allow *'
        prun $SUDO "$CEPH_BIN/ceph-authtool" --gen-key --name=client.admin \
             --cap mon 'allow *' \
             --cap osd 'allow *' \
             --cap mds 'allow *' \
             --cap mgr 'allow *' \
             "$keyring_fn"

        # build a fresh fs monmap, mon fs
        local params=()
        local count=0
        local mon_host=""
        for f in $MONS
        do
            if [ $msgr -eq 1 ]; then
                A="v1:$IP:$(($CEPH_PORT+$count+1))"
            fi
            if [ $msgr -eq 2 ]; then
                A="v2:$IP:$(($CEPH_PORT+$count+1))"
            fi
            if [ $msgr -eq 21 ]; then
                A="[v2:$IP:$(($CEPH_PORT+$count)),v1:$IP:$(($CEPH_PORT+$count+1))]"
            fi
            params+=("--addv" "$f" "$A")
            mon_host="$mon_host $A"
            wconf <<EOF
[mon.$f]
        host = $HOSTNAME
        mon data = $CEPH_DEV_DIR/mon.$f
EOF
            count=$(($count + 2))
        done
        wconf <<EOF
[global]
        mon host = $mon_host
EOF
        prun "$CEPH_BIN/monmaptool" --create --clobber "${params[@]}" --print "$monmap_fn"

        for f in $MONS
        do
            prun rm -rf -- "$CEPH_DEV_DIR/mon.$f"
            prun mkdir -p "$CEPH_DEV_DIR/mon.$f"
            prun "$CEPH_BIN/ceph-mon" --mkfs -c "$conf_fn" -i "$f" --monmap="$monmap_fn" --keyring="$keyring_fn"
        done

        prun rm -- "$monmap_fn"
    fi

    # start monitors
    for f in $MONS
    do
        run 'mon' $f $CEPH_BIN/ceph-mon -i $f $ARGS $CMON_ARGS
    done

    if [ "$crimson" -eq 1 ]; then
        $CEPH_BIN/ceph osd set-allow-crimson --yes-i-really-mean-it
    fi

    if [ -n "$require_osd_and_client_version" ]; then
        $CEPH_BIN/ceph osd set-require-min-compat-client $require_osd_and_client_version
        $CEPH_BIN/ceph osd require-osd-release $require_osd_and_client_version --yes-i-really-mean-it
    fi

    if [ -n "$use_crush_tunables" ]; then
        $CEPH_BIN/ceph osd crush tunables $use_crush_tunables
    fi
}

start_osd() {
    if [ $inc_osd_num -gt 0 ]; then
        old_maxosd=$($CEPH_BIN/ceph osd getmaxosd | sed -e 's/max_osd = //' -e 's/ in epoch.*//')
        start=$old_maxosd
        end=$(($start-1+$inc_osd_num))
        overwrite_conf=1 # fake wconf
    else
        start=0
        end=$(($CEPH_NUM_OSD-1))
    fi
    local osds_wait
    for osd in `seq $start $end`
    do
	if [ "$ceph_osd" == "crimson-osd" ]; then
        bottom_cpu=$(( osd * crimson_smp ))
        top_cpu=$(( bottom_cpu + crimson_smp - 1 ))
	    # set exclusive CPU nodes for each osd
	    echo "$CEPH_BIN/ceph -c $conf_fn config set osd.$osd crimson_seastar_cpu_cores $bottom_cpu-$top_cpu"
	    $CEPH_BIN/ceph -c $conf_fn config set "osd.$osd" crimson_seastar_cpu_cores "$bottom_cpu-$top_cpu"
	fi
	if [ "$new" -eq 1 -o $inc_osd_num -gt 0 ]; then
            wconf <<EOF
[osd.$osd]
        host = $HOSTNAME
EOF

            if [ "$osds_per_host" -gt 0 ]; then
                wconf <<EOF
        crush location = root=default host=$HOSTNAME-$(echo "$osd / $osds_per_host" | bc)
EOF
            fi

            if [ "$spdk_enabled" -eq 1 ]; then
                wconf <<EOF
        bluestore_block_path = spdk:${bluestore_spdk_dev[$osd]}
EOF
            elif [ "$pmem_enabled" -eq 1 ]; then
                wconf <<EOF
        bluestore_block_path = ${bluestore_pmem_file}
EOF
            fi
            rm -rf $CEPH_DEV_DIR/osd$osd || true
            if command -v btrfs > /dev/null; then
                for f in $CEPH_DEV_DIR/osd$osd/*; do btrfs sub delete $f &> /dev/null || true; done
            fi
            if [ -n "$kstore_path" ]; then
                ln -s $kstore_path $CEPH_DEV_DIR/osd$osd
            else
                mkdir -p $CEPH_DEV_DIR/osd$osd
                if [ -n "${block_devs[$osd]}" ]; then
                    dd if=/dev/zero of=${block_devs[$osd]} bs=1M count=1
                    ln -s ${block_devs[$osd]} $CEPH_DEV_DIR/osd$osd/block
                fi
                if [ -n "${bluestore_db_devs[$osd]}" ]; then
                    dd if=/dev/zero of=${bluestore_db_devs[$osd]} bs=1M count=1
                    ln -s ${bluestore_db_devs[$osd]} $CEPH_DEV_DIR/osd$osd/block.db
                fi
                if [ -n "${bluestore_wal_devs[$osd]}" ]; then
                    dd if=/dev/zero of=${bluestore_wal_devs[$osd]} bs=1M count=1
                    ln -s ${bluestore_wal_devs[$osd]} $CEPH_DEV_DIR/osd$osd/block.wal
                fi
                if [ -n "${secondary_block_devs[$osd]}" ]; then
                    dd if=/dev/zero of=${secondary_block_devs[$osd]} bs=1M count=1
                    mkdir -p $CEPH_DEV_DIR/osd$osd/block.${secondary_block_devs_type}.1
                    ln -s ${secondary_block_devs[$osd]} $CEPH_DEV_DIR/osd$osd/block.${secondary_block_devs_type}.1/block
                fi
            fi
            if [ "$objectstore" == "bluestore" ]; then
                wconf <<EOF
        bluestore fsck on mount = false
EOF
            fi

            local uuid=`uuidgen`
            echo "add osd$osd $uuid"
            OSD_SECRET=$($CEPH_BIN/ceph-authtool --gen-print-key)
            echo "{\"cephx_secret\": \"$OSD_SECRET\"}" > $CEPH_DEV_DIR/osd$osd/new.json
            ceph_adm osd new $uuid -i $CEPH_DEV_DIR/osd$osd/new.json
            rm $CEPH_DEV_DIR/osd$osd/new.json
            prun $SUDO $CEPH_BIN/$ceph_osd $extra_osd_args -i $osd $ARGS --mkfs --key $OSD_SECRET --osd-uuid $uuid $extra_seastar_args \
                2>&1 | tee $CEPH_OUT_DIR/osd-mkfs.$osd.log

            local key_fn=$CEPH_DEV_DIR/osd$osd/keyring
            cat > $key_fn<<EOF
[osd.$osd]
        key = $OSD_SECRET
EOF
        fi
        echo start osd.$osd
        local osd_pid
        echo 'osd' $osd $SUDO $CEPH_BIN/$ceph_osd \
            $extra_seastar_args $extra_osd_args \
            -i $osd $ARGS $COSD_ARGS
        run 'osd' $osd $SUDO $CEPH_BIN/$ceph_osd \
            $extra_seastar_args $extra_osd_args \
            -i $osd $ARGS $COSD_ARGS &
        osd_pid=$!
        if $parallel; then
            osds_wait=$osd_pid
        else
            wait $osd_pid
        fi
    done
    if $parallel; then
        for p in $osds_wait; do
            wait $p
        done
        debug echo OSDs started
    fi
    if [ $inc_osd_num -gt 0 ]; then
        # update num osd
        new_maxosd=$($CEPH_BIN/ceph osd getmaxosd | sed -e 's/max_osd = //' -e 's/ in epoch.*//')
        sed -i "s/num osd = .*/num osd = $new_maxosd/g" $conf_fn
    fi
}

create_mgr_restful_secret() {
    while ! ceph_adm -h | grep -c -q ^restful ; do
        debug echo 'waiting for mgr restful module to start'
        sleep 1
    done
    local secret_file
    if ceph_adm restful create-self-signed-cert > /dev/null; then
        secret_file=`mktemp`
        ceph_adm restful create-key admin -o $secret_file
        RESTFUL_SECRET=`cat $secret_file`
        rm $secret_file
    else
        debug echo MGR Restful is not working, perhaps the package is not installed?
    fi
}

start_mgr() {
    local mgr=0
    local ssl=${DASHBOARD_SSL:-1}
    # avoid monitors on nearby ports (which test/*.sh use extensively)
    MGR_PORT=$(($CEPH_PORT + 1000))
    PROMETHEUS_PORT=9283
    for name in x y z a b c d e f g h i j k l m n o p
    do
        [ $mgr -eq $CEPH_NUM_MGR ] && break
        mgr=$(($mgr + 1))
        if [ "$new" -eq 1 ]; then
            mkdir -p $CEPH_DEV_DIR/mgr.$name
            key_fn=$CEPH_DEV_DIR/mgr.$name/keyring
            $SUDO $CEPH_BIN/ceph-authtool --create-keyring --gen-key --name=mgr.$name $key_fn
            ceph_adm -i $key_fn auth add mgr.$name mon 'allow profile mgr' mds 'allow *' osd 'allow *'

            wconf <<EOF
[mgr.$name]
        host = $HOSTNAME
EOF

            if $with_mgr_dashboard ; then
                local port_option="ssl_server_port"
                local http_proto="https"
                if [ "$ssl" == "0" ]; then
                    port_option="server_port"
                    http_proto="http"
                    ceph_adm config set mgr mgr/dashboard/ssl false --force
                fi
                ceph_adm config set mgr mgr/dashboard/$name/$port_option $MGR_PORT --force
                if [ $mgr -eq 1 ]; then
                    DASH_URLS="$http_proto://$IP:$MGR_PORT"
                else
                    DASH_URLS+=", $http_proto://$IP:$MGR_PORT"
                fi
            fi
	    MGR_PORT=$(($MGR_PORT + 1000))
	    ceph_adm config set mgr mgr/prometheus/$name/server_port $PROMETHEUS_PORT --force
	    PROMETHEUS_PORT=$(($PROMETHEUS_PORT + 1000))

	    ceph_adm config set mgr mgr/restful/$name/server_port $MGR_PORT --force
            if [ $mgr -eq 1 ]; then
                RESTFUL_URLS="https://$IP:$MGR_PORT"
            else
                RESTFUL_URLS+=", https://$IP:$MGR_PORT"
            fi
	    MGR_PORT=$(($MGR_PORT + 1000))
        fi

        debug echo "Starting mgr.${name}"
        run 'mgr' $name $CEPH_BIN/ceph-mgr -i $name $ARGS
    done

    while ! ceph_adm mgr stat | jq -e '.available'; do
        debug echo 'waiting for mgr to become available'
        sleep 1
    done
    
    if [ "$new" -eq 1 ]; then
        # setting login credentials for dashboard
        if $with_mgr_dashboard; then
            while ! ceph_adm -h | grep -c -q ^dashboard ; do
                debug echo 'waiting for mgr dashboard module to start'
                sleep 1
            done
            DASHBOARD_ADMIN_SECRET_FILE="${CEPH_CONF_PATH}/dashboard-admin-secret.txt"
            printf 'admin' > "${DASHBOARD_ADMIN_SECRET_FILE}"
            ceph_adm dashboard ac-user-create admin -i "${DASHBOARD_ADMIN_SECRET_FILE}" \
                administrator --force-password
            if [ "$ssl" != "0" ]; then
                if ! ceph_adm dashboard create-self-signed-cert;  then
                    debug echo dashboard module not working correctly!
                fi
            fi

            ceph_adm osd pool create rbd
            ceph_adm osd pool application enable rbd rbd

            if [ -n "${NVMEOF_GW}" ]; then
                echo "Adding nvmeof-gateway ${NVMEOF_GW} to dashboard"
                ceph_adm dashboard nvmeof-gateway-add -i <(echo "${NVMEOF_GW}") "${NVMEOF_GW/:/_}"
            fi
        fi
        if $with_mgr_restful; then
            create_mgr_restful_secret
        fi
    fi

    if [ "$cephadm" -eq 1 ]; then
        debug echo Enabling cephadm orchestrator
	if [ "$new" -eq 1 ]; then
		digest=$(curl -s \
		https://hub.docker.com/v2/repositories/ceph/daemon-base/tags/latest-master-devel \
		| jq -r '.images[0].digest')
		ceph_adm config set global container_image "docker.io/ceph/daemon-base@$digest"
	fi
        ceph_adm config-key set mgr/cephadm/ssh_identity_key -i ~/.ssh/id_rsa
        ceph_adm config-key set mgr/cephadm/ssh_identity_pub -i ~/.ssh/id_rsa.pub
        ceph_adm mgr module enable cephadm
        ceph_adm orch set backend cephadm
        ceph_adm orch host add "$(hostname)"
        ceph_adm orch apply crash '*'
        ceph_adm config set mgr mgr/cephadm/allow_ptrace true
    fi
}

start_mds() {
    local mds=0
    for name in a b c d e f g h i j k l m n o p
    do
        [ $mds -eq $CEPH_NUM_MDS ] && break
        mds=$(($mds + 1))

        if [ "$new" -eq 1 ]; then
            prun mkdir -p "$CEPH_DEV_DIR/mds.$name"
            key_fn=$CEPH_DEV_DIR/mds.$name/keyring
            wconf <<EOF
[mds.$name]
        host = $HOSTNAME
EOF
            if [ "$standby" -eq 1 ]; then
                mkdir -p $CEPH_DEV_DIR/mds.${name}s
                wconf <<EOF
        mds standby for rank = $mds
[mds.${name}s]
        mds standby replay = true
        mds standby for name = ${name}
EOF
            fi
            prun $SUDO "$CEPH_BIN/ceph-authtool" --create-keyring --gen-key --name="mds.$name" "$key_fn"
            ceph_adm -i "$key_fn" auth add "mds.$name" mon 'allow profile mds' osd 'allow rw tag cephfs *=*' mds 'allow' mgr 'allow profile mds'
            if [ "$standby" -eq 1 ]; then
                prun $SUDO "$CEPH_BIN/ceph-authtool" --create-keyring --gen-key --name="mds.${name}s" \
                     "$CEPH_DEV_DIR/mds.${name}s/keyring"
                ceph_adm -i "$CEPH_DEV_DIR/mds.${name}s/keyring" auth add "mds.${name}s" \
                             mon 'allow profile mds' osd 'allow *' mds 'allow' mgr 'allow profile mds'
            fi
        fi

        run 'mds' $name $CEPH_BIN/ceph-mds -i $name $ARGS $CMDS_ARGS
        if [ "$standby" -eq 1 ]; then
            run 'mds' $name $CEPH_BIN/ceph-mds -i ${name}s $ARGS $CMDS_ARGS
        fi

        #valgrind --tool=massif $CEPH_BIN/ceph-mds $ARGS --mds_log_max_segments 2 --mds_thrash_fragments 0 --mds_thrash_exports 0 > m  #--debug_ms 20
        #$CEPH_BIN/ceph-mds -d $ARGS --mds_thrash_fragments 0 --mds_thrash_exports 0 #--debug_ms 20
        #ceph_adm mds set max_mds 2
    done

    if [ $new -eq 1 ]; then
        if [ "$CEPH_NUM_FS" -gt "0" ] ; then
            sleep 5 # time for MDS to come up as standby to avoid health warnings on fs creation
            if [ "$CEPH_NUM_FS" -gt "1" ] ; then
                ceph_adm fs flag set enable_multiple true --yes-i-really-mean-it
            fi

	    # wait for volume module to load
	    while ! ceph_adm fs volume ls ; do sleep 1 ; done
            local fs=0
            for name in a b c d e f g h i j k l m n o p
            do
                ceph_adm fs volume create ${name}
                ceph_adm fs authorize ${name} "client.fs_${name}" / rwp >> "$keyring_fn"
                fs=$(($fs + 1))
                [ $fs -eq $CEPH_NUM_FS ] && break
            done
        fi
    fi

}

# Ganesha Daemons requires nfs-ganesha nfs-ganesha-ceph nfs-ganesha-rados-grace
# nfs-ganesha-rados-urls (version 3.3 and above) packages installed. On
# Fedora>=31 these packages can be installed directly with 'dnf'. For CentOS>=8
# the packages are available at
# https://wiki.centos.org/SpecialInterestGroup/Storage
# Similarly for Ubuntu>=16.04 follow the instructions on
# https://launchpad.net/~nfs-ganesha

start_ganesha() {
    cluster_id="vstart"
    GANESHA_PORT=$(($CEPH_PORT + 4000))
    local ganesha=0
    test_user="$cluster_id"
    pool_name=".nfs"
    namespace=$cluster_id
    url="rados://$pool_name/$namespace/conf-nfs.$test_user"

    prun ceph_adm auth get-or-create client.$test_user \
        mon "allow r" \
        osd "allow rw pool=$pool_name namespace=$namespace, allow rw tag cephfs data=a" \
        mds "allow rw path=/" \
        >> "$keyring_fn"

    ceph_adm mgr module enable test_orchestrator
    ceph_adm orch set backend test_orchestrator
    ceph_adm test_orchestrator load_data -i $CEPH_ROOT/src/pybind/mgr/test_orchestrator/dummy_data.json
    prun ceph_adm nfs cluster create $cluster_id
    prun ceph_adm nfs export create cephfs --fsname "a" --cluster-id $cluster_id --pseudo-path "/cephfs"

    for name in a b c d e f g h i j k l m n o p
    do
        [ $ganesha -eq $GANESHA_DAEMON_NUM ] && break

        port=$(($GANESHA_PORT + ganesha))
        ganesha=$(($ganesha + 1))
        ganesha_dir="$CEPH_DEV_DIR/ganesha.$name"
        prun rm -rf $ganesha_dir
        prun mkdir -p $ganesha_dir

        echo "NFS_CORE_PARAM {
            Enable_NLM = false;
            Enable_RQUOTA = false;
            Protocols = 4;
            NFS_Port = $port;
        }

        MDCACHE {
           Dir_Chunk = 0;
        }

        NFSv4 {
           RecoveryBackend = rados_cluster;
           Minor_Versions = 1, 2;
        }

        RADOS_KV {
           pool = '$pool_name';
           namespace = $namespace;
           UserId = $test_user;
           nodeid = $name;
        }

        RADOS_URLS {
	   Userid = $test_user;
	   watch_url = '$url';
        }

	%url $url" > "$ganesha_dir/ganesha-$name.conf"
	wconf <<EOF
[ganesha.$name]
        host = $HOSTNAME
        ip = $IP
        port = $port
        ganesha data = $ganesha_dir
        pid file = $CEPH_OUT_DIR/ganesha-$name.pid
EOF

        prun env CEPH_CONF="${conf_fn}" ganesha-rados-grace --userid $test_user -p $pool_name -n $namespace add $name
        prun env CEPH_CONF="${conf_fn}" ganesha-rados-grace --userid $test_user -p $pool_name -n $namespace

        prun env CEPH_CONF="${conf_fn}" ganesha.nfsd -L "$CEPH_OUT_DIR/ganesha-$name.log" -f "$ganesha_dir/ganesha-$name.conf" -p "$CEPH_OUT_DIR/ganesha-$name.pid" -N NIV_EVENT

        # Wait few seconds for grace period to be removed
        sleep 2

        prun env CEPH_CONF="${conf_fn}" ganesha-rados-grace --userid $test_user -p $pool_name -n $namespace

        echo "$test_user ganesha daemon $name started on port: $port"
    done
}

if [ "$debug" -eq 0 ]; then
    CMONDEBUG='
        debug mon = 10
        debug ms = 1'
    CCLIENTDEBUG=''
    CMDSDEBUG=''
else
    debug echo "** going verbose **"
    CMONDEBUG='
        debug osd = 20
        debug mon = 20
        debug osd = 20
        debug paxos = 20
        debug auth = 20
        debug mgrc = 20
        debug ms = 1'
    CCLIENTDEBUG='
        debug client = 20'
    CMDSDEBUG='
        debug mds = 20'
fi

# Crimson doesn't support PG merge/split yet.
if [ "$ceph_osd" == "crimson-osd" ]; then
    AUTOSCALER_OPTS='
        osd_pool_default_pg_autoscale_mode = off'
fi

if [ -n "$MON_ADDR" ]; then
    CMON_ARGS=" -m "$MON_ADDR
    COSD_ARGS=" -m "$MON_ADDR
    CMDS_ARGS=" -m "$MON_ADDR
fi

if [ -z "$CEPH_PORT" ]; then
    while [ true ]
    do
        CEPH_PORT="$(echo $(( RANDOM % 1000 + 40000 )))"
        ss -a -n | egrep "\<LISTEN\>.+:${CEPH_PORT}\s+" 1>/dev/null 2>&1 || break
    done
fi

[ -z "$INIT_CEPH" ] && INIT_CEPH=$CEPH_BIN/init-ceph

# sudo if btrfs
[ -d $CEPH_DEV_DIR/osd0/. ] && [ -e $CEPH_DEV_DIR/sudo ] && SUDO="sudo"

if [ $inc_osd_num -eq 0 ]; then
    prun $SUDO rm -f core*
fi

[ -d $CEPH_ASOK_DIR ] || mkdir -p $CEPH_ASOK_DIR
[ -d $CEPH_OUT_DIR  ] || mkdir -p $CEPH_OUT_DIR
[ -d $CEPH_DEV_DIR  ] || mkdir -p $CEPH_DEV_DIR
[ -d $CEPH_OUT_CLIENT_DIR ] || mkdir -p $CEPH_OUT_CLIENT_DIR
if [ $inc_osd_num -eq 0 ]; then
    $SUDO find "$CEPH_OUT_DIR" -type f -delete
fi
[ -d gmon ] && $SUDO rm -rf gmon/*

[ "$cephx" -eq 1 ] && [ "$new" -eq 1 ] && [ -e $keyring_fn ] && rm $keyring_fn


# figure machine's ip
HOSTNAME=`hostname -s`
if [ -n "$ip" ]; then
    IP="$ip"
else
    echo hostname $HOSTNAME
    if [ -x "$(which ip 2>/dev/null)" ]; then
        IP_CMD="ip addr"
    else
        IP_CMD="ifconfig"
    fi
    # filter out IPv4 and localhost addresses
    IP="$($IP_CMD | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p' | head -n1)"
    # if nothing left, try using localhost address, it might work
    if [ -z "$IP" ]; then IP="127.0.0.1"; fi
fi
echo "ip $IP"
echo "port $CEPH_PORT"


[ -z $CEPH_ADM ] && CEPH_ADM=$CEPH_BIN/ceph

ceph_adm() {
    if [ "$cephx" -eq 1 ]; then
        prun $SUDO "$CEPH_ADM" -c "$conf_fn" -k "$keyring_fn" "$@"
    else
        prun $SUDO "$CEPH_ADM" -c "$conf_fn" "$@"
    fi
}

if [ $inc_osd_num -gt 0 ]; then
    start_osd
    exit
fi

if [ "$new" -eq 1 ]; then
    prepare_conf
fi

if [ $CEPH_NUM_MON -gt 0 ]; then
    start_mon

    debug echo Populating config ...
    cat <<EOF | $CEPH_BIN/ceph -c $conf_fn config assimilate-conf -i -
[global]
osd_pool_default_size = $OSD_POOL_DEFAULT_SIZE
osd_pool_default_min_size = 1

[mon]
mon_osd_reporter_subtree_level = osd
mon_data_avail_warn = 2
mon_data_avail_crit = 1
mon_allow_pool_delete = true
mon_allow_pool_size_one = true

[osd]
osd_scrub_load_threshold = 2000
osd_debug_op_order = true
osd_debug_misdirected_ops = true
osd_copyfrom_max_chunk = 524288

[mgr]
mgr/telemetry/nag = false
mgr/telemetry/enable = false

EOF

    if [ "$debug" -ne 0 ]; then
        debug echo Setting debug configs ...
        cat <<EOF | $CEPH_BIN/ceph -c $conf_fn config assimilate-conf -i -
[mgr]
debug_ms = 1
debug_mgr = 20
debug_monc = 20
debug_mon = 20

[osd]
debug_ms = 1
debug_osd = 25
debug_objecter = 20
debug_monc = 20
debug_mgrc = 20
debug_journal = 20
debug_bluestore = 20
debug_bluefs = 20
debug_rocksdb = 20
debug_bdev = 20
debug_reserver = 10
debug_objclass = 20

[mds]
debug_ms = 1
debug_mds = 20
debug_monc = 20
debug_mgrc = 20
mds_debug_scatterstat = true
mds_verify_scatter = true
mds_debug_frag = true
mds_debug_auth_pins = true
mds_debug_subtrees = true
EOF
    fi
    if [ "$cephadm" -gt 0 ]; then
        debug echo Setting mon public_network ...
        public_network=$(ip route list | grep -w "$IP" | grep -v default | grep -E "/[0-9]+" | awk '{print $1}')
        ceph_adm config set mon public_network $public_network
    fi
fi

if [ "$ceph_osd" == "crimson-osd" ]; then
     if [ "$debug" -ne 0 ]; then
        extra_seastar_args=" --debug"
    fi
    if [ "$trace" -ne 0 ]; then
        extra_seastar_args=" --trace"
    fi
    if [ "$(expr $(nproc) - 1)" -gt "$(($CEPH_NUM_OSD * crimson_smp))" ]; then
        if [ $crimson_alien_num_cores -gt 0 ]; then
            alien_bottom_cpu=$(($CEPH_NUM_OSD * crimson_smp))
            alien_top_cpu=$(( alien_bottom_cpu + crimson_alien_num_cores - 1 ))
            # Ensure top value within range:
            if [ "$(($alien_top_cpu))" -gt "$(expr $(nproc) - 1)" ]; then
                alien_top_cpu=$(expr $(nproc) - 1)
            fi
            echo "crimson_alien_thread_cpu_cores: $alien_bottom_cpu-$alien_top_cpu"
            # This is a (logical) processor id range, it could be refined to encompass only physical processor ids
            # (equivalently, ignore hyperthreading sibling processor ids)
            $CEPH_BIN/ceph -c $conf_fn config set osd crimson_alien_thread_cpu_cores "$alien_bottom_cpu-$alien_top_cpu"
        else
            echo "crimson_alien_thread_cpu_cores:" $(($CEPH_NUM_OSD * crimson_smp))-"$(expr $(nproc) - 1)"
            $CEPH_BIN/ceph -c $conf_fn config set osd crimson_alien_thread_cpu_cores $(($CEPH_NUM_OSD * crimson_smp))-"$(expr $(nproc) - 1)"
        fi
        if [ $crimson_alien_num_threads -gt 0 ]; then
            echo "$CEPH_BIN/ceph -c $conf_fn config set osd crimson_alien_op_num_threads $crimson_alien_num_threads"
            $CEPH_BIN/ceph -c $conf_fn config set osd crimson_alien_op_num_threads "$crimson_alien_num_threads"
        fi
    else
      echo "No alien thread cpu core isolation"
    fi
fi

if [ $CEPH_NUM_MGR -gt 0 ]; then
    start_mgr
fi

# osd
if [ $CEPH_NUM_OSD -gt 0 ]; then
    start_osd
fi

# mds
if [ "$smallmds" -eq 1 ]; then
    wconf <<EOF
[mds]
        mds log max segments = 2
        # Default 'mds cache memory limit' is 1GiB, and here we set it to 100MiB.
        mds cache memory limit = 100M
EOF
fi

if [ $CEPH_NUM_MDS -gt 0 ]; then
    start_mds
    # key with access to all FS
    ceph_adm fs authorize \* "client.fs" / rwp >> "$keyring_fn"
fi

# Don't set max_mds until all the daemons are started, otherwise
# the intended standbys might end up in active roles.
if [ "$CEPH_MAX_MDS" -gt 1 ]; then
    sleep 5  # wait for daemons to make it into FSMap before increasing max_mds
fi
fs=0
for name in a b c d e f g h i j k l m n o p
do
    [ $fs -eq $CEPH_NUM_FS ] && break
    fs=$(($fs + 1))
    if [ "$CEPH_MAX_MDS" -gt 1 ]; then
        ceph_adm fs set "${name}" max_mds "$CEPH_MAX_MDS"
    fi
done

# mgr

if [ "$ec" -eq 1 ]; then
    ceph_adm <<EOF
osd erasure-code-profile set ec-profile m=2 k=2
osd pool create ec erasure ec-profile
EOF
fi

do_cache() {
    while [ -n "$*" ]; do
        p="$1"
        shift
        debug echo "creating cache for pool $p ..."
        ceph_adm <<EOF
osd pool create ${p}-cache
osd tier add $p ${p}-cache
osd tier cache-mode ${p}-cache writeback
osd tier set-overlay $p ${p}-cache
EOF
    done
}
do_cache $cache

do_hitsets() {
    while [ -n "$*" ]; do
        pool="$1"
        type="$2"
        shift
        shift
        debug echo "setting hit_set on pool $pool type $type ..."
        ceph_adm <<EOF
osd pool set $pool hit_set_type $type
osd pool set $pool hit_set_count 8
osd pool set $pool hit_set_period 30
EOF
    done
}
do_hitsets $hitset

do_rgw_create_bucket()
{
   # Create RGW Bucket
   local rgw_python_file='rgw-create-bucket.py'
   echo "import boto
import boto.s3.connection

conn = boto.connect_s3(
        aws_access_key_id = '$s3_akey',
        aws_secret_access_key = '$s3_skey',
        host = '$HOSTNAME',
        port = 80,
        is_secure=False,
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

bucket = conn.create_bucket('nfs-bucket')
print('created new bucket')" > "$CEPH_OUT_DIR/$rgw_python_file"
   prun python $CEPH_OUT_DIR/$rgw_python_file
}

do_rgw_create_users()
{
    # Create S3 user
    s3_akey='0555b35654ad1656d804'
    s3_skey='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
    debug echo "setting up user testid"
    $CEPH_BIN/radosgw-admin user create --uid testid --access-key $s3_akey --secret $s3_skey --display-name 'M. Tester' --email tester@ceph.com -c $conf_fn > /dev/null

    # Create S3-test users
    # See: https://github.com/ceph/s3-tests
    debug echo "setting up s3-test users"

    $CEPH_BIN/radosgw-admin user create \
        --uid 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
        --access-key ABCDEFGHIJKLMNOPQRST \
        --secret abcdefghijklmnopqrstuvwxyzabcdefghijklmn \
        --display-name youruseridhere \
        --email s3@example.com --caps="roles=*;user-policy=*" -c $conf_fn > /dev/null
    $CEPH_BIN/radosgw-admin user create \
        --uid 56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234 \
        --access-key NOPQRSTUVWXYZABCDEFG \
        --secret nopqrstuvwxyzabcdefghijklmnabcdefghijklm \
        --display-name john.doe \
        --email john.doe@example.com -c $conf_fn > /dev/null
    $CEPH_BIN/radosgw-admin user create \
	--tenant testx \
        --uid 9876543210abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
        --access-key HIJKLMNOPQRSTUVWXYZA \
        --secret opqrstuvwxyzabcdefghijklmnopqrstuvwxyzab \
        --display-name tenanteduser \
        --email tenanteduser@example.com -c $conf_fn > /dev/null

    if [ "$rgw_store" == "rados" ] ; then
        # create accounts/users for iam s3tests
        a1_akey='AAAAAAAAAAAAAAAAAAaa'
        a1_skey='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        $CEPH_BIN/radosgw-admin account create --account-id RGW11111111111111111 --account-name Account1 --email account1@ceph.com -c $conf_fn > /dev/null
        $CEPH_BIN/radosgw-admin user create --account-id RGW11111111111111111 --uid testacct1root --account-root \
            --display-name 'Account1Root' --access-key $a1_akey --secret $a1_skey -c $conf_fn > /dev/null

        a2_akey='BBBBBBBBBBBBBBBBBBbb'
        a2_skey='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
        $CEPH_BIN/radosgw-admin account create --account-id RGW22222222222222222 --account-name Account2 --email account2@ceph.com -c $conf_fn > /dev/null
        $CEPH_BIN/radosgw-admin user create --account-id RGW22222222222222222 --uid testacct2root --account-root \
            --display-name 'Account2Root' --access-key $a2_akey --secret $a2_skey -c $conf_fn > /dev/null

        a1u_akey='CCCCCCCCCCCCCCCCCCcc'
        a1u_skey='cccccccccccccccccccccccccccccccccccccccc'
        $CEPH_BIN/radosgw-admin user create --account-id RGW11111111111111111 --uid testacct1user \
            --display-name 'Account1User' --access-key $a1u_akey --secret $a1u_skey -c $conf_fn > /dev/null
        $CEPH_BIN/radosgw-admin user policy attach --uid testacct1user \
            --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess -c $conf_fn > /dev/null
    fi

    # Create Swift user
    debug echo "setting up user tester"
    $CEPH_BIN/radosgw-admin user create -c $conf_fn --subuser=test:tester --display-name=Tester-Subuser --key-type=swift --secret=testing --access=full > /dev/null

    echo ""
    echo "S3 User Info:"
    echo "  access key:  $s3_akey"
    echo "  secret key:  $s3_skey"
    echo ""
    echo "Swift User Info:"
    echo "  account   : test"
    echo "  user      : tester"
    echo "  password  : testing"
    echo ""
}

do_rgw()
{
    if [ "$new" -eq 1 ]; then
        do_rgw_create_users
        if [ -n "$rgw_compression" ]; then
            debug echo "setting compression type=$rgw_compression"
            $CEPH_BIN/radosgw-admin zone placement modify -c $conf_fn --rgw-zone=default --placement-id=default-placement --compression=$rgw_compression > /dev/null
        fi
    fi

    if [ -n "$rgw_flight_frontend" ] ;then
        debug echo "starting arrow_flight frontend on first rgw"
    fi

    # Start server
    if [ "$cephadm" -gt 0 ]; then
        ceph_adm orch apply rgw rgwTest
        return
    fi

    RGWDEBUG=""
    if [ "$debug" -ne 0 ]; then
        RGWDEBUG="--debug-rgw=20 --debug-ms=1"
    fi

    local CEPH_RGW_PORT_NUM="${CEPH_RGW_PORT}"
    local CEPH_RGW_HTTPS="${CEPH_RGW_PORT: -1}"
    if [[ "${CEPH_RGW_HTTPS}" = "s" ]]; then
        CEPH_RGW_PORT_NUM="${CEPH_RGW_PORT::-1}"
    else
        CEPH_RGW_HTTPS=""
    fi
    RGWSUDO=
    [ $CEPH_RGW_PORT_NUM -lt 1024 ] && RGWSUDO=sudo

    current_port=$CEPH_RGW_PORT
    # allow only first rgw to start arrow_flight server/port
    local flight_conf=$rgw_flight_frontend
    for n in $(seq 1 $CEPH_NUM_RGW); do
        rgw_name="client.rgw.${current_port}"

        if [ "$CEPH_NUM_MON" -gt 0 ]; then
            ceph_adm auth get-or-create $rgw_name \
                mon 'allow rw' \
                osd 'allow rwx' \
                mgr 'allow rw' \
                >> "$keyring_fn"
        fi

        debug echo start rgw on http${CEPH_RGW_HTTPS}://localhost:${current_port}
        run 'rgw' $current_port $RGWSUDO $CEPH_BIN/radosgw -c $conf_fn \
            --log-file=${CEPH_OUT_DIR}/radosgw.${current_port}.log \
            --admin-socket=${CEPH_OUT_DIR}/radosgw.${current_port}.asok \
            --pid-file=${CEPH_OUT_DIR}/radosgw.${current_port}.pid \
            --rgw_luarocks_location=${CEPH_OUT_DIR}/radosgw.${current_port}.luarocks  \
            ${RGWDEBUG} \
            -n ${rgw_name} \
            "--rgw_frontends=${rgw_frontend} port=${current_port}${CEPH_RGW_HTTPS}${flight_conf:+,arrow_flight}"

        i=$(($i + 1))
        [ $i -eq $CEPH_NUM_RGW ] && break

        current_port=$((current_port+1))
        unset flight_conf
    done
}
if [ "$CEPH_NUM_RGW" -gt 0 ]; then
    do_rgw
fi

# Ganesha Daemons
if [ $GANESHA_DAEMON_NUM -gt 0 ]; then
    pseudo_path="/cephfs"
    if [ "$cephadm" -gt 0 ]; then
        cluster_id="vstart"
	port="2049"
        prun ceph_adm nfs cluster create $cluster_id
	if [ $CEPH_NUM_MDS -gt 0 ]; then
            prun ceph_adm nfs export create cephfs --fsname "a" --cluster-id $cluster_id --pseudo-path $pseudo_path
	    echo "Mount using: mount -t nfs -o port=$port $IP:$pseudo_path mountpoint"
	fi
	if [ "$CEPH_NUM_RGW" -gt 0 ]; then
            pseudo_path="/rgw"
            do_rgw_create_bucket
	    prun ceph_adm nfs export create rgw --cluster-id $cluster_id --pseudo-path $pseudo_path --bucket "nfs-bucket"
            echo "Mount using: mount -t nfs -o port=$port $IP:$pseudo_path mountpoint"
	fi
    else
        start_ganesha
	echo "Mount using: mount -t nfs -o port=<ganesha-port-num> $IP:$pseudo_path mountpoint"
    fi
fi

docker_service(){
     local service=''
     #prefer podman
     if command -v podman > /dev/null; then
	 service="podman"
     elif pgrep -f docker > /dev/null; then
	 service="docker"
     fi
     if [ -n "$service" ]; then
       echo "using $service for deploying jaeger..."
       #check for exited container, remove them and restart container
       if [ "$($service ps -aq -f status=exited -f name=jaeger)" ]; then
	 $service rm jaeger
       fi
       if [ ! "$(podman ps -aq -f name=jaeger)" ]; then
         $service "$@"
       fi
     else
         echo "cannot find docker or podman, please restart service and rerun."
     fi
}

echo ""
if [ $with_jaeger -eq 1 ]; then
    debug echo "Enabling jaegertracing..."
    docker_service run -d --name jaeger \
  -p 5775:5775/udp \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 14268:14268 \
  -p 14250:14250 \
  quay.io/jaegertracing/all-in-one
fi

debug echo "vstart cluster complete. Use stop.sh to stop. See out/* (e.g. 'tail -f out/????') for debug output."

echo ""
if [ "$new" -eq 1 ]; then
    if $with_mgr_dashboard; then
        cat <<EOF
dashboard urls: $DASH_URLS
  w/ user/pass: admin / admin
EOF
    fi
    if $with_mgr_restful; then
        cat <<EOF
restful urls: $RESTFUL_URLS
  w/ user/pass: admin / $RESTFUL_SECRET
EOF
    fi
fi

echo ""
# add header to the environment file
{
    echo "#"
    echo "# source this file into your shell to set up the environment."
    echo "# For example:"
    echo "# $ . $CEPH_DIR/vstart_environment.sh"
    echo "#"
} > $CEPH_DIR/vstart_environment.sh
{
    echo "export PYTHONPATH=$PYBIND:$CYTHON_PYTHONPATH:$CEPH_PYTHON_COMMON\$PYTHONPATH"
    echo "export LD_LIBRARY_PATH=$CEPH_LIB:\$LD_LIBRARY_PATH"
    echo "export PATH=$CEPH_DIR/bin:\$PATH"
    echo "export CEPH_CONF=$conf_fn"
    # We cannot set CEPH_KEYRING if this is sourced by vstart_runner.py (API tests)
    if [ "$CEPH_DIR" != "$PWD" ]; then
        echo "export CEPH_KEYRING=$keyring_fn"
    fi

    if [ -n "$CEPHFS_SHELL" ]; then
        echo "alias cephfs-shell=$CEPHFS_SHELL"
    fi
} | tee -a $CEPH_DIR/vstart_environment.sh

echo "CEPH_DEV=1"

# always keep this section at the very bottom of this file
STRAY_CONF_PATH="/etc/ceph/ceph.conf"
if [ -f "$STRAY_CONF_PATH" -a -n "$conf_fn" -a ! "$conf_fn" -ef "$STRAY_CONF_PATH" ]; then
    echo ""
    echo ""
    echo "WARNING:"
    echo "    Please remove stray $STRAY_CONF_PATH if not needed."
    echo "    Your conf files $conf_fn and $STRAY_CONF_PATH may not be in sync"
    echo "    and may lead to undesired results."
    echo ""
    echo "NOTE:"
    echo "    Remember to restart cluster after removing $STRAY_CONF_PATH"
fi

init_logrotate
