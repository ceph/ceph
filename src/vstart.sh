#!/usr/bin/env bash

# abort on failure
set -e

if [ -n "$VSTART_DEST" ]; then
  SRC_PATH=`dirname $0`
  SRC_PATH=`(cd $SRC_PATH; pwd)`

  CEPH_DIR=$SRC_PATH
  CEPH_BIN=${PWD}/bin
  CEPH_LIB=${PWD}/lib

  CEPH_CONF_PATH=$VSTART_DEST
  CEPH_DEV_DIR=$VSTART_DEST/dev
  CEPH_OUT_DIR=$VSTART_DEST/out
  CEPH_ASOK_DIR=$VSTART_DEST/out
fi

get_cmake_variable() {
    local variable=$1
    grep "$variable" CMakeCache.txt | cut -d "=" -f 2
}

# for running out of the CMake build directory
if [ -e CMakeCache.txt ]; then
  # Out of tree build, learn source location from CMakeCache.txt
  CEPH_ROOT=$(get_cmake_variable ceph_SOURCE_DIR)
  CEPH_BUILD_DIR=`pwd`
  [ -z "$MGR_PYTHON_PATH" ] && MGR_PYTHON_PATH=$CEPH_ROOT/src/pybind/mgr
  CEPH_MGR_PY_VERSION_MAJOR=$(get_cmake_variable MGR_PYTHON_VERSION | cut -d '.' -f1)
  if [ -n "$CEPH_MGR_PY_VERSION_MAJOR" ]; then
      CEPH_PY_VERSION_MAJOR=${CEPH_MGR_PY_VERSION_MAJOR}
  else
      if [ $(get_cmake_variable WITH_PYTHON2) = ON ]; then
          CEPH_PY_VERSION_MAJOR=2
      else
          CEPH_PY_VERSION_MAJOR=3
      fi
  fi
fi

# use CEPH_BUILD_ROOT to vstart from a 'make install' 
if [ -n "$CEPH_BUILD_ROOT" ]; then
        [ -z "$CEPH_BIN" ] && CEPH_BIN=$CEPH_BUILD_ROOT/bin
        [ -z "$CEPH_LIB" ] && CEPH_LIB=$CEPH_BUILD_ROOT/lib
        [ -z "$EC_PATH" ] && EC_PATH=$CEPH_LIB/erasure-code
        [ -z "$OBJCLASS_PATH" ] && OBJCLASS_PATH=$CEPH_LIB/rados-classes
        # make install should install python extensions into PYTHONPATH
elif [ -n "$CEPH_ROOT" ]; then
        [ -z "$PYBIND" ] && PYBIND=$CEPH_ROOT/src/pybind
        [ -z "$CEPH_BIN" ] && CEPH_BIN=$CEPH_BUILD_DIR/bin
        [ -z "$CEPH_ADM" ] && CEPH_ADM=$CEPH_BIN/ceph
        [ -z "$INIT_CEPH" ] && INIT_CEPH=$CEPH_BUILD_DIR/bin/init-ceph
        [ -z "$CEPH_LIB" ] && CEPH_LIB=$CEPH_BUILD_DIR/lib
        [ -z "$OBJCLASS_PATH" ] && OBJCLASS_PATH=$CEPH_LIB
        [ -z "$EC_PATH" ] && EC_PATH=$CEPH_LIB
fi

if [ -z "${CEPH_VSTART_WRAPPER}" ]; then
    PATH=$(pwd):$PATH
fi

[ -z "$PYBIND" ] && PYBIND=./pybind

export PYTHONPATH=$PYBIND:$CEPH_LIB/cython_modules/lib.${CEPH_PY_VERSION_MAJOR}:$PYTHONPATH
export LD_LIBRARY_PATH=$CEPH_LIB:$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=$CEPH_LIB:$DYLD_LIBRARY_PATH
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

# if none of the CEPH_NUM_* number is specified, kill the existing
# cluster.
if [ -z "$CEPH_NUM_MON" -a \
     -z "$CEPH_NUM_OSD" -a \
     -z "$CEPH_NUM_MDS" -a \
     -z "$CEPH_NUM_MGR" ]; then
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

[ -z "$CEPH_DIR" ] && CEPH_DIR="$PWD"
[ -z "$CEPH_DEV_DIR" ] && CEPH_DEV_DIR="$CEPH_DIR/dev"
[ -z "$CEPH_OUT_DIR" ] && CEPH_OUT_DIR="$CEPH_DIR/out"
[ -z "$CEPH_RGW_PORT" ] && CEPH_RGW_PORT=8000
[ -z "$CEPH_CONF_PATH" ] && CEPH_CONF_PATH=$CEPH_DIR

if [ $CEPH_NUM_OSD -gt 3 ]; then
    OSD_POOL_DEFAULT_SIZE=3
else
    OSD_POOL_DEFAULT_SIZE=$CEPH_NUM_OSD
fi

extra_conf=""
new=0
standby=0
debug=0
ip=""
nodaemon=0
smallmds=0
short=0
ec=0
hitset=""
overwrite_conf=1
cephx=1 #turn cephx on by default
cache=""
if [ `uname` = FreeBSD ]; then
    objectstore="filestore"
else
    objectstore="bluestore"
fi
rgw_frontend="civetweb"
rgw_compression=""
lockdep=${LOCKDEP:-1}

with_mgr_dashboard=true
if [[ "$(get_cmake_variable WITH_MGR_DASHBOARD_FRONTEND)" != "ON" ]]; then
  echo "ceph-mgr dashboard not built - disabling."
  with_mgr_dashboard=false
fi

filestore_path=
kstore_path=

VSTART_SEC="client.vstart.sh"

MON_ADDR=""
DASH_URLS=""
RESTFUL_URLS=""

conf_fn="$CEPH_CONF_PATH/ceph.conf"
keyring_fn="$CEPH_CONF_PATH/keyring"
osdmap_fn="/tmp/ceph_osdmap.$$"
monmap_fn="/tmp/ceph_monmap.$$"

usage="usage: $0 [option]... \nex: MON=3 OSD=1 MDS=1 MGR=1 RGW=1 $0 -n -d\n"
usage=$usage"options:\n"
usage=$usage"\t-d, --debug\n"
usage=$usage"\t-s, --standby_mds: Generate standby-replay MDS for each active\n"
usage=$usage"\t-l, --localhost: use localhost instead of hostname\n"
usage=$usage"\t-i <ip>: bind to specific ip\n"
usage=$usage"\t-n, --new\n"
usage=$usage"\t-N, --not-new: reuse existing cluster config (default)\n"
usage=$usage"\t--valgrind[_{osd,mds,mon,rgw}] 'toolname args...'\n"
usage=$usage"\t--nodaemon: use ceph-run as wrapper for mon/osd/mds\n"
usage=$usage"\t--smallmds: limit mds cache size\n"
usage=$usage"\t-m ip:port\t\tspecify monitor address\n"
usage=$usage"\t-k keep old configuration files\n"
usage=$usage"\t-x enable cephx (on by default)\n"
usage=$usage"\t-X disable cephx\n"
usage=$usage"\t--hitset <pool> <hit_set_type>: enable hitset tracking\n"
usage=$usage"\t-e : create an erasure pool\n";
usage=$usage"\t-o config\t\t add extra config parameters to all sections\n"
usage=$usage"\t--rgw_port specify ceph rgw http listen port\n"
usage=$usage"\t--rgw_frontend specify the rgw frontend configuration\n"
usage=$usage"\t--rgw_compression specify the rgw compression plugin\n"
usage=$usage"\t-b, --bluestore use bluestore as the osd objectstore backend (default)\n"
usage=$usage"\t-f, --filestore use filestore as the osd objectstore backend\n"
usage=$usage"\t-K, --kstore use kstore as the osd objectstore backend\n"
usage=$usage"\t--memstore use memstore as the osd objectstore backend\n"
usage=$usage"\t--cache <pool>: enable cache tiering on pool\n"
usage=$usage"\t--short: short object names only; necessary for ext4 dev\n"
usage=$usage"\t--nolockdep disable lockdep\n"
usage=$usage"\t--multimds <count> allow multimds with maximum active count\n"
usage=$usage"\t--without-dashboard: do not run using mgr dashboard\n"

usage_exit() {
	printf "$usage"
	exit
}

while [ $# -ge 1 ]; do
case $1 in
    -d | --debug )
	    debug=1
	    ;;
    -s | --standby_mds)
	    standby=1
	    ;;
    -l | --localhost )
	    ip="127.0.0.1"
	    ;;
    -i )
	    [ -z "$2" ] && usage_exit
	    ip="$2"
	    shift
	    ;;
    -e )
	    ec=1
	    ;;
    --new | -n )
	    new=1
	    ;;
    --not-new | -N )
	new=0
	;;
    --short )
	    short=1
	    ;;
    --valgrind )
	    [ -z "$2" ] && usage_exit
	    valgrind=$2
	    shift
	    ;;
    --valgrind_args )
	valgrind_args="$2"
	shift
	;;
    --valgrind_mds )
	    [ -z "$2" ] && usage_exit
	    valgrind_mds=$2
	    shift
	    ;;
    --valgrind_osd )
	    [ -z "$2" ] && usage_exit
	    valgrind_osd=$2
	    shift
	    ;;
    --valgrind_mon )
	    [ -z "$2" ] && usage_exit
	    valgrind_mon=$2
	    shift
	    ;;
    --valgrind_mgr )
	    [ -z "$2" ] && usage_exit
	    valgrind_mgr=$2
	    shift
	    ;;
    --valgrind_rgw )
	    [ -z "$2" ] && usage_exit
	    valgrind_rgw=$2
	    shift
	    ;;
    --nodaemon )
	    nodaemon=1
	    ;;
    --smallmds )
	    smallmds=1
	    ;;
    --rgw_port )
            CEPH_RGW_PORT=$2
            shift
            ;;
    --rgw_frontend )
            rgw_frontend=$2
            shift
            ;;
    --rgw_compression )
            rgw_compression=$2
            shift
            ;;
    --kstore_path )
	kstore_path=$2
	shift
	;;
    --filestore_path )
	filestore_path=$2
	shift
	;;
    -m )
	    [ -z "$2" ] && usage_exit
	    MON_ADDR=$2
	    shift
	    ;;
    -x )
	    cephx=1 # this is on be default, flag exists for historical consistency
	    ;;
    -X )
	    cephx=0
	    ;;
    -k )
	    if [ ! -r $conf_fn ]; then
	        echo "cannot use old configuration: $conf_fn not readable." >&2
	        exit
	    fi
	    overwrite_conf=0
	    ;;
    --memstore )
	    objectstore="memstore"
	    ;;
    -b | --bluestore )
	    objectstore="bluestore"
	    ;;
    -f | --filestore )
	    objectstore="filestore"
	    ;;
    -K | --kstore )
            objectstore="kstore"
            ;;
    --hitset )
	    hitset="$hitset $2 $3"
	    shift
	    shift
	    ;;
    -o )
	    extra_conf="$extra_conf	$2
"
	    shift
	    ;;
    --cache )
	    if [ -z "$cache" ]; then
		cache="$2"
	    else
		cache="$cache $2"
	    fi
	    shift
	    ;;
    --nolockdep )
            lockdep=0
            ;;
    --multimds)
        CEPH_MAX_MDS="$2"
        shift
        ;;
    --without-dashboard)
        with_mgr_dashboard=false
        ;;
    * )
	    usage_exit
esac
shift
done

if [ $kill_all -eq 1 ]; then
    $SUDO $INIT_CEPH stop
fi

if [ "$overwrite_conf" -eq 0 ]; then
    CEPH_ASOK_DIR=`dirname $($CEPH_BIN/ceph-conf  -c $conf_fn --show-config-value admin_socket)`
    mkdir -p $CEPH_ASOK_DIR
    MON=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC num_mon 2>/dev/null` && \
        CEPH_NUM_MON="$MON"
    OSD=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC num_osd 2>/dev/null` && \
        CEPH_NUM_OSD="$OSD"
    MDS=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC num_mds 2>/dev/null` && \
        CEPH_NUM_MDS="$MDS"
    MGR=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC num_mgr 2>/dev/null` && \
        CEPH_NUM_MGR="$MGR"
    RGW=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC num_rgw 2>/dev/null` && \
        CEPH_NUM_RGW="$RGW"
else
    if [ "$new" -ne 0 ]; then
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
    else
	if [ -z "$CEPH_ASOK_DIR" ]; then
            CEPH_ASOK_DIR=`dirname $($CEPH_BIN/ceph-conf -c $conf_fn --show-config-value admin_socket)`
        fi
        # -k is implied... (doesn't make sense otherwise)
        overwrite_conf=0
    fi
fi

ARGS="-c $conf_fn"

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

prunb() {
    quoted_print "$@" '&'
    "$@" &
}

prun() {
    quoted_print "$@"
    "$@"
}

run() {
    type=$1
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
            prunb ./ceph-run "$@" -f
        fi
    fi
}

wconf() {
	if [ "$overwrite_conf" -eq 1 ]; then
		cat >> "$conf_fn"
	fi
}

prepare_conf() {
    local DAEMONOPTS="
        log file = $CEPH_OUT_DIR/\$name.log
        admin socket = $CEPH_ASOK_DIR/\$name.asok
        chdir = \"\"
        pid file = $CEPH_OUT_DIR/\$name.pid
        heartbeat file = $CEPH_OUT_DIR/\$name.heartbeat
"

    local mgr_modules="restful status balancer iostat devicehealth"
    if $with_mgr_dashboard; then
      mgr_modules="dashboard $mgr_modules"
    fi

    wconf <<EOF
; generated by vstart.sh on `date`
[$VSTART_SEC]
        num mon = $CEPH_NUM_MON
        num osd = $CEPH_NUM_OSD
        num mds = $CEPH_NUM_MDS
        num mgr = $CEPH_NUM_MGR
        num rgw = $CEPH_NUM_RGW

[global]
        fsid = $(uuidgen)
        osd failsafe full ratio = .99
        mon osd full ratio = .99
        mon osd nearfull ratio = .99
        mon osd backfillfull ratio = .99
        erasure code dir = $EC_PATH
        plugin dir = $CEPH_LIB
        filestore fd cache size = 32
        run dir = $CEPH_OUT_DIR
        enable experimental unrecoverable data corrupting features = *
	osd_crush_chooseleaf_type = 0
$extra_conf
EOF
	if [ "$lockdep" -eq 1 ] ; then
		wconf <<EOF
        lockdep = true
EOF
	fi
	if [ "$cephx" -ne 1 ] ; then
		wconf <<EOF
	auth cluster required = none
	auth service required = none
	auth client required = none
EOF
	fi
	if [ "$short" -eq 1 ]; then
		COSDSHORT="        osd max object name len = 460
        osd max object namespace len = 64"
	fi
	wconf <<EOF
[client]
        keyring = $keyring_fn
        log file = $CEPH_OUT_DIR/\$name.\$pid.log
        admin socket = $CEPH_ASOK_DIR/\$name.\$pid.asok
$extra_conf
[client.rgw]
        rgw frontends = $rgw_frontend port=$CEPH_RGW_PORT
        ; needed for s3tests
        rgw crypt s3 kms encryption keys = testkey-1=YmluCmJvb3N0CmJvb3N0LWJ1aWxkCmNlcGguY29uZgo= testkey-2=aWIKTWFrZWZpbGUKbWFuCm91dApzcmMKVGVzdGluZwo=
        rgw crypt require ssl = false
        rgw lc debug interval = 10

[mds]
$DAEMONOPTS
        mds data = $CEPH_DEV_DIR/mds.\$id
        mds root ino uid = `id -u`
        mds root ino gid = `id -g`
$extra_conf
[mgr]
        mgr data = $CEPH_DEV_DIR/mgr.\$id
        mgr module path = $MGR_PYTHON_PATH
$DAEMONOPTS
$extra_conf
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

        filestore wbthrottle xfs ios start flusher = 10
        filestore wbthrottle xfs ios hard limit = 20
        filestore wbthrottle xfs inodes hard limit = 30
        filestore wbthrottle btrfs ios start flusher = 10
        filestore wbthrottle btrfs ios hard limit = 20
        filestore wbthrottle btrfs inodes hard limit = 30
        bluestore fsck on mount = true
        bluestore block create = true
	bluestore block db path = $CEPH_DEV_DIR/osd\$id/block.db.file
        bluestore block db size = 67108864
        bluestore block db create = true
	bluestore block wal path = $CEPH_DEV_DIR/osd\$id/block.wal.file
        bluestore block wal size = 1048576000
        bluestore block wal create = true

        ; kstore
        kstore fsck on mount = true
        osd objectstore = $objectstore
$COSDSHORT
$extra_conf
[mon]
        mgr initial modules = $mgr_modules
$DAEMONOPTS
$CMONDEBUG
$extra_conf
        mon cluster log file = $CEPH_OUT_DIR/cluster.mon.\$id.log
        osd pool default erasure code profile = plugin=jerasure technique=reed_sol_van k=2 m=1 crush-failure-domain=osd
EOF
}

start_mon() {
    local MONS=""
    local count=0
    for f in a b c d e f g h i j k l m n o p q r s t u v w x y z
    do
        [ $count -eq $CEPH_NUM_MON ] && break;
        count=$(($count + 1))
        if [ -z "$MONS" ];
        then
	        MONS="$f"
        else
	        MONS="$MONS $f"
        fi
    done

    if [ "$new" -eq 1 ]; then
		if [ `echo $IP | grep '^127\\.'` ]
		then
			echo
			echo "NOTE: hostname resolves to loopback; remote hosts will not be able to"
			echo "  connect.  either adjust /etc/hosts, or edit this script to use your"
			echo "  machine's real IP."
			echo
		fi

		prun $SUDO "$CEPH_BIN/ceph-authtool" --create-keyring --gen-key --name=mon. "$keyring_fn" --cap mon 'allow *'
		prun $SUDO "$CEPH_BIN/ceph-authtool" --gen-key --name=client.admin --set-uid=0 \
			--cap mon 'allow *' \
			--cap osd 'allow *' \
			--cap mds 'allow *' \
			--cap mgr 'allow *' \
			"$keyring_fn"

		prun $SUDO "$CEPH_BIN/ceph-authtool" --gen-key --name=client.fs --set-uid=0 \
		   --cap mon 'allow r' \
			--cap osd 'allow rw tag cephfs data=*' \
			--cap mds 'allow rwp' \
			"$keyring_fn"

		prun $SUDO "$CEPH_BIN/ceph-authtool" --gen-key --name=client.rgw \
		    --cap mon 'allow rw' \
		    --cap osd 'allow rwx' \
		    --cap mgr 'allow rw' \
		    "$keyring_fn"

		# build a fresh fs monmap, mon fs
		local str=""
		local count=0
		for f in $MONS
		do
			str="$str --add $f $IP:$(($CEPH_PORT+$count))"
			wconf <<EOF
[mon.$f]
        host = $HOSTNAME
        mon data = $CEPH_DEV_DIR/mon.$f
        mon addr = $IP:$(($CEPH_PORT+$count))
EOF
			count=$(($count + 1))
		done
		prun "$CEPH_BIN/monmaptool" --create --clobber $str --print "$monmap_fn"

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
		run 'mon' $CEPH_BIN/ceph-mon -i $f $ARGS $CMON_ARGS
	done
}

start_osd() {
    for osd in `seq 0 $((CEPH_NUM_OSD-1))`
    do
	    if [ "$new" -eq 1 ]; then
		    wconf <<EOF
[osd.$osd]
        host = $HOSTNAME
EOF

            rm -rf $CEPH_DEV_DIR/osd$osd || true
            if command -v btrfs > /dev/null; then
                for f in $CEPH_DEV_DIR/osd$osd/*; do btrfs sub delete $f &> /dev/null || true; done
            fi
	    if [ -n "$filestore_path" ]; then
		ln -s $filestore_path $CEPH_DEV_DIR/osd$osd
	    elif [ -n "$kstore_path" ]; then
		ln -s $kstore_path $CEPH_DEV_DIR/osd$osd
	    else
		mkdir -p $CEPH_DEV_DIR/osd$osd
	    fi

            local uuid=`uuidgen`
            echo "add osd$osd $uuid"
	    OSD_SECRET=$($CEPH_BIN/ceph-authtool --gen-print-key)
	    echo "{\"cephx_secret\": \"$OSD_SECRET\"}" > $CEPH_DEV_DIR/osd$osd/new.json
            ceph_adm osd new $uuid -i $CEPH_DEV_DIR/osd$osd/new.json
	    rm $CEPH_DEV_DIR/osd$osd/new.json
            $SUDO $CEPH_BIN/ceph-osd -i $osd $ARGS --mkfs --key $OSD_SECRET --osd-uuid $uuid

            local key_fn=$CEPH_DEV_DIR/osd$osd/keyring
	    cat > $key_fn<<EOF
[osd.$osd]
	key = $OSD_SECRET
EOF
            echo adding osd$osd key to auth repository
            ceph_adm -i "$key_fn" auth add osd.$osd osd "allow *" mon "allow profile osd" mgr "allow profile osd"
        fi
        echo start osd.$osd
        run 'osd' $SUDO $CEPH_BIN/ceph-osd -i $osd $ARGS $COSD_ARGS
    done
}

start_mgr() {
    local mgr=0
    # avoid monitors on nearby ports (which test/*.sh use extensively)
    MGR_PORT=$(($CEPH_PORT + 1000))
    for name in x y z a b c d e f g h i j k l m n o p
    do
        [ $mgr -eq $CEPH_NUM_MGR ] && break
        mgr=$(($mgr + 1))
        if [ "$new" -eq 1 ]; then
            mkdir -p $CEPH_DEV_DIR/mgr.$name
            key_fn=$CEPH_DEV_DIR/mgr.$name/keyring
            $SUDO $CEPH_BIN/ceph-authtool --create-keyring --gen-key --name=mgr.$name $key_fn
            ceph_adm -i $key_fn auth add mgr.$name mon 'allow profile mgr' mds 'allow *' osd 'allow *'
        fi

        wconf <<EOF
[mgr.$name]
        host = $HOSTNAME
EOF

        if $with_mgr_dashboard ; then
            ceph_adm config set mgr mgr/dashboard/$name/server_port $MGR_PORT
            if [ $mgr -eq 1 ]; then
                DASH_URLS="https://$IP:$MGR_PORT"
            else
                DASH_URLS+=", https://$IP:$MGR_PORT"
            fi
        fi
	MGR_PORT=$(($MGR_PORT + 1000))

	ceph_adm config set mgr mgr/restful/$name/server_port $MGR_PORT
        if [ $mgr -eq 1 ]; then
            RESTFUL_URLS="https://$IP:$MGR_PORT"
        else
            RESTFUL_URLS+=", https://$IP:$MGR_PORT"
        fi
	MGR_PORT=$(($MGR_PORT + 1000))

        echo "Starting mgr.${name}"
        run 'mgr' $CEPH_BIN/ceph-mgr -i $name $ARGS
    done

    # use tell mgr here because the first mgr might not have activated yet
    # to register the python module commands.

    # setting login credentials for dashboard
    if $with_mgr_dashboard; then
        ceph_adm tell mgr dashboard set-login-credentials admin admin
        if ! ceph_adm tell mgr dashboard create-self-signed-cert;  then
            echo dashboard module not working correctly!
        fi
    fi

    if ceph_adm tell mgr restful create-self-signed-cert; then
        SF=`mktemp`
        ceph_adm restful create-key admin -o $SF
        RESTFUL_SECRET=`cat $SF`
        rm $SF
    else 
        echo MGR Restful is not working, perhaps the package is not installed?
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

	    run 'mds' $CEPH_BIN/ceph-mds -i $name $ARGS $CMDS_ARGS
	    if [ "$standby" -eq 1 ]; then
	        run 'mds' $CEPH_BIN/ceph-mds -i ${name}s $ARGS $CMDS_ARGS
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

            local fs=0
            for name in a b c d e f g h i j k l m n o p
            do
                ceph_adm osd pool create "cephfs_data_${name}" 8
                ceph_adm osd pool create "cephfs_metadata_${name}" 8
                ceph_adm fs new "cephfs_${name}" "cephfs_metadata_${name}" "cephfs_data_${name}"
					 ceph_adm fs authorize "cephfs_${name}" "client.fs_${name}" / rwp
                fs=$(($fs + 1))
                [ $fs -eq $CEPH_NUM_FS ] && break
            done
        fi
    fi

}

if [ "$debug" -eq 0 ]; then
    CMONDEBUG='
        debug mon = 10
        debug ms = 1'
else
    echo "** going verbose **"
    CMONDEBUG='
        debug mon = 20
        debug paxos = 20
        debug auth = 20
	debug mgrc = 20
        debug ms = 1'
fi

if [ -n "$MON_ADDR" ]; then
	CMON_ARGS=" -m "$MON_ADDR
	COSD_ARGS=" -m "$MON_ADDR
	CMDS_ARGS=" -m "$MON_ADDR
fi

if [ -z "$CEPH_PORT" ]
then
  while [ true ]
  do
    CEPH_PORT="$(echo $(( RANDOM % 1000 + 40000 )))"
    ss -a -n | egrep ":${CEPH_PORT} .+LISTEN" 1>/dev/null 2>&1 || break
  done
fi

[ -z "$INIT_CEPH" ] && INIT_CEPH=$CEPH_BIN/init-ceph

# sudo if btrfs
test -d $CEPH_DEV_DIR/osd0/. && test -e $CEPH_DEV_DIR/sudo && SUDO="sudo"

prun $SUDO rm -f core*

test -d $CEPH_ASOK_DIR || mkdir $CEPH_ASOK_DIR
test -d $CEPH_OUT_DIR || mkdir $CEPH_OUT_DIR
test -d $CEPH_DEV_DIR || mkdir $CEPH_DEV_DIR
$SUDO rm -rf $CEPH_OUT_DIR/*
test -d gmon && $SUDO rm -rf gmon/*

[ "$cephx" -eq 1 ] && [ "$new" -eq 1 ] && test -e $keyring_fn && rm $keyring_fn


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
    # filter out IPv6 and localhost addresses
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

if [ "$new" -eq 1 ]; then
    prepare_conf
fi

if [ $CEPH_NUM_MON -gt 0 ]; then
    start_mon

    echo Populating config ...
    cat <<EOF | $CEPH_BIN/ceph -c $conf_fn config assimilate-conf -i -
[global]
osd_pool_default_size = $OSD_POOL_DEFAULT_SIZE
osd_pool_default_min_size = 1
mon_pg_warn_min_per_osd = 3

[mon]
mon_osd_reporter_subtree_level = osd
mon_data_avail_warn = 2
mon_data_avail_crit = 1
mon_allow_pool_delete = true

[osd]
osd_scrub_load_threshold = 2000
osd_debug_op_order = true
osd_debug_misdirected_ops = true
osd_copyfrom_max_chunk = 524288

[mds]
mds_debug_frag = true
mds_debug_auth_pins = true
mds_debug_subtrees = true

EOF

    if [ "$debug" -ne 0 ]; then
	echo Setting debug configs ...
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
debug_filestore = 20
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
EOF
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
	mds cache size = 10000
EOF
fi

if [ $CEPH_NUM_MDS -gt 0 ]; then
    start_mds
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
        ceph_adm fs set "cephfs_${name}" max_mds "$CEPH_MAX_MDS"
    fi
done

# mgr

if [ "$ec" -eq 1 ]; then
    ceph_adm <<EOF
osd erasure-code-profile set ec-profile m=2 k=2
osd pool create ec 8 8 erasure ec-profile
EOF
fi

do_cache() {
    while [ -n "$*" ]; do
	p="$1"
	shift
	echo "creating cache for pool $p ..."
	ceph_adm <<EOF
osd pool create ${p}-cache 8
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
	echo "setting hit_set on pool $pool type $type ..."
	ceph_adm <<EOF
osd pool set $pool hit_set_type $type
osd pool set $pool hit_set_count 8
osd pool set $pool hit_set_period 30
EOF
    done
}
do_hitsets $hitset

do_rgw_create_users()
{
    # Create S3 user
    local akey='0555b35654ad1656d804'
    local skey='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
    echo "setting up user testid"
    $CEPH_BIN/radosgw-admin user create --uid testid --access-key $akey --secret $skey --display-name 'M. Tester' --email tester@ceph.com -c $conf_fn > /dev/null

    # Create S3-test users
    # See: https://github.com/ceph/s3-tests
    echo "setting up s3-test users"
    $CEPH_BIN/radosgw-admin user create \
        --uid 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
        --access-key ABCDEFGHIJKLMNOPQRST \
        --secret abcdefghijklmnopqrstuvwxyzabcdefghijklmn \
        --display-name youruseridhere \
        --email s3@example.com -c $conf_fn > /dev/null
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

    # Create Swift user
    echo "setting up user tester"
    $CEPH_BIN/radosgw-admin user create -c $conf_fn --subuser=test:tester --display-name=Tester-Subuser --key-type=swift --secret=testing --access=full > /dev/null

    echo ""
    echo "S3 User Info:"
    echo "  access key:  $akey"
    echo "  secret key:  $skey"
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
            echo "setting compression type=$rgw_compression"
            $CEPH_BIN/radosgw-admin zone placement modify -c $conf_fn --rgw-zone=default --placement-id=default-placement --compression=$rgw_compression > /dev/null
        fi
    fi
    # Start server
    RGWDEBUG=""
    if [ "$debug" -ne 0 ]; then
        RGWDEBUG="--debug-rgw=20"
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
    n=$(($CEPH_NUM_RGW - 1))
    i=0
    for rgw in j k l m n o p q r s t u v; do
        current_port=$((CEPH_RGW_PORT_NUM + i))
        echo start rgw on http${CEPH_RGW_HTTPS}://localhost:${current_port}
        run 'rgw' $RGWSUDO $CEPH_BIN/radosgw -c $conf_fn --log-file=${CEPH_OUT_DIR}/radosgw.${current_port}.log --admin-socket=${CEPH_OUT_DIR}/radosgw.${current_port}.asok --pid-file=${CEPH_OUT_DIR}/radosgw.${current_port}.pid ${RGWDEBUG} --debug-ms=1 -n client.rgw "--rgw_frontends=${rgw_frontend} port=${current_port}${CEPH_RGW_HTTPS}"
        i=$(($i + 1))
        [ $i -eq $CEPH_NUM_RGW ] && break
    done
}
if [ "$CEPH_NUM_RGW" -gt 0 ]; then
    do_rgw
fi

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

echo ""
if $with_mgr_dashboard; then
    echo "dashboard urls: $DASH_URLS"
    echo "  w/ user/pass: admin / admin"
fi
echo "restful urls: $RESTFUL_URLS"
echo "  w/ user/pass: admin / $RESTFUL_SECRET"
echo ""
echo ""
echo "export PYTHONPATH=./pybind:$PYTHONPATH"
echo "export LD_LIBRARY_PATH=$CEPH_LIB"

if [ "$CEPH_DIR" != "$PWD" ]; then
    echo "export CEPH_CONF=$conf_fn"
    echo "export CEPH_KEYRING=$keyring_fn"
fi

echo "CEPH_DEV=1"

