#!/bin/sh

# abort on failure
set -e

if [ -n "$VSTART_DEST" ]; then
  SRC_PATH=`dirname $0`
  SRC_PATH=`(cd $SRC_PATH; pwd)`

  CEPH_DIR=$SRC_PATH
  CEPH_BIN=$SRC_PATH
  CEPH_LIB=$SRC_PATH/.libs

  if [ -e CMakeCache.txt ]; then
      CEPH_BIN=$VSTART_DEST/../../bin
      CEPH_LIB=$VSTART_DEST/../../lib
  fi

  CEPH_CONF_PATH=$VSTART_DEST
  CEPH_DEV_DIR=$VSTART_DEST/dev
  CEPH_OUT_DIR=$VSTART_DEST/out
fi

# for running out of the CMake build directory
if [ -e CMakeCache.txt ]; then
  # Out of tree build, learn source location from CMakeCache.txt
  CEPH_ROOT=`grep Ceph_SOURCE_DIR CMakeCache.txt | cut -d "=" -f 2`
  CEPH_BUILD_DIR=`pwd`
fi

# use CEPH_BUILD_ROOT to vstart from a 'make install' 
if [ -n "$CEPH_BUILD_ROOT" ]; then
        [ -z $CEPH_BIN ] && CEPH_BIN=$CEPH_BUILD_ROOT/bin
        [ -z $CEPH_LIB ] && CEPH_LIB=$CEPH_BUILD_ROOT/lib
        [ -z $EC_PATH ] && EC_PATH=$CEPH_LIB/erasure-code
        [ -z $CS_PATH ] && CS_PATH=$CEPH_LIB/compressor
        [ -z $OBJCLASS_PATH ] && OBJCLASS_PATH=$CEPH_LIB/rados-classes
elif [ -n "$CEPH_ROOT" ]; then
        [ -z "$PYBIND" ] && PYBIND=$CEPH_ROOT/src/pybind
        [ -z "$CEPH_BIN" ] && CEPH_BIN=$CEPH_BUILD_DIR/bin
        [ -z "$CEPH_ADM" ] && CEPH_ADM=$CEPH_BIN/ceph
        [ -z "$INIT_CEPH" ] && INIT_CEPH=$CEPH_BIN/init-ceph
        [ -z "$CEPH_LIB" ] && CEPH_LIB=$CEPH_BUILD_DIR/lib
        [ -z "$OBJCLASS_PATH" ] && OBJCLASS_PATH=$CEPH_LIB
        [ -z "$EC_PATH" ] && EC_PATH=$CEPH_LIB
else
        [ -z "$CEPH_BIN" ] && CEPH_BIN=.
        [ -z "$CEPH_LIB" ] && CEPH_LIB=.libs
        [ -z $EC_PATH ] && EC_PATH=$CEPH_LIB
        [ -z $CS_PATH ] && CS_PATH=$CEPH_LIB
        [ -z $OBJCLASS_PATH ] && OBJCLASS_PATH=$CEPH_LIB
fi

if [ -z "${CEPH_VSTART_WRAPPER}" ]; then
    PATH=$(pwd):$PATH
fi

[ -z "$PYBIND" ] && PYBIND=./pybind

export PYTHONPATH=$PYBIND:$PYTHONPATH
export LD_LIBRARY_PATH=$CEPH_LIB:$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=$CEPH_LIB:$DYLD_LIBRARY_PATH

[ -z "$CEPH_NUM_MON" ] && CEPH_NUM_MON="$MON"
[ -z "$CEPH_NUM_OSD" ] && CEPH_NUM_OSD="$OSD"
[ -z "$CEPH_NUM_MDS" ] && CEPH_NUM_MDS="$MDS"
[ -z "$CEPH_NUM_FS"  ] && CEPH_NUM_FS="$FS"
[ -z "$CEPH_NUM_RGW" ] && CEPH_NUM_RGW="$RGW"

[ -z "$CEPH_NUM_MON" ] && CEPH_NUM_MON=3
[ -z "$CEPH_NUM_OSD" ] && CEPH_NUM_OSD=3
[ -z "$CEPH_NUM_MDS" ] && CEPH_NUM_MDS=3
[ -z "$CEPH_NUM_FS"  ] && CEPH_NUM_FS=1
[ -z "$CEPH_NUM_RGW" ] && CEPH_NUM_RGW=1

[ -z "$CEPH_DIR" ] && CEPH_DIR="$PWD"
[ -z "$CEPH_DEV_DIR" ] && CEPH_DEV_DIR="$CEPH_DIR/dev"
[ -z "$CEPH_OUT_DIR" ] && CEPH_OUT_DIR="$CEPH_DIR/out"
[ -z "$CEPH_RGW_PORT" ] && CEPH_RGW_PORT=8000
[ -z "$CEPH_CONF_PATH" ] && CEPH_CONF_PATH=$CEPH_DIR

extra_conf=""
new=0
standby=0
debug=0
start_all=1
start_mon=0
start_mds=0
start_osd=0
start_rgw=0
ip=""
nodaemon=0
smallmds=0
ec=0
hitset=""
overwrite_conf=1
cephx=1 #turn cephx on by default
cache=""
memstore=0
bluestore=0

VSTART_SEC="client.vstart.sh"

MON_ADDR=""

conf_fn="$CEPH_CONF_PATH/ceph.conf"
keyring_fn="$CEPH_CONF_PATH/keyring"
osdmap_fn="/tmp/ceph_osdmap.$$"
monmap_fn="/tmp/ceph_monmap.$$"

usage="usage: $0 [option]... [\"mon\"] [\"mds\"] [\"osd\"]\n"
usage=$usage"options:\n"
usage=$usage"\t-d, --debug\n"
usage=$usage"\t-s, --standby_mds: Generate standby-replay MDS for each active\n"
usage=$usage"\t-l, --localhost: use localhost instead of hostname\n"
usage=$usage"\t-i <ip>: bind to specific ip\n"
usage=$usage"\t-r start radosgw (needs ceph compiled with --radosgw)\n"
usage=$usage"\t-n, --new\n"
usage=$usage"\t--valgrind[_{osd,mds,mon}] 'toolname args...'\n"
usage=$usage"\t--nodaemon: use ceph-run as wrapper for mon/osd/mds\n"
usage=$usage"\t--smallmds: limit mds cache size\n"
usage=$usage"\t-m ip:port\t\tspecify monitor address\n"
usage=$usage"\t-k keep old configuration files\n"
usage=$usage"\t-x enable cephx (on by default)\n"
usage=$usage"\t-X disable cephx\n"
usage=$usage"\t--hitset <pool> <hit_set_type>: enable hitset tracking\n"
usage=$usage"\t-e : create an erasure pool\n";
usage=$usage"\t-o config\t\t add extra config parameters to all sections\n"
usage=$usage"\t--mon_num specify ceph monitor count\n"
usage=$usage"\t--osd_num specify ceph osd count\n"
usage=$usage"\t--mds_num specify ceph mds count\n"
usage=$usage"\t--rgw_port specify ceph rgw http listen port\n"
usage=$usage"\t--bluestore use bluestore as the osd objectstore backend\n"
usage=$usage"\t--memstore use memstore as the osd objectstore backend\n"
usage=$usage"\t--cache <pool>: enable cache tiering on pool\n"

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
    -r )
	    start_rgw=1
	    ;;
    -e )
	    ec=1
	    ;;
    --new | -n )
	    new=1
	    ;;
    --valgrind )
	    [ -z "$2" ] && usage_exit
	    valgrind=$2
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
    --nodaemon )
	    nodaemon=1
	    ;;
    --smallmds )
	    smallmds=1
	    ;;
    --mon_num )
            echo "mon_num:$2"
            CEPH_NUM_MON="$2"
            shift
            ;;
    --osd_num )
            CEPH_NUM_OSD=$2
            shift
            ;;
    --mds_num )
            CEPH_NUM_MDS=$2
            shift
            ;;
    --rgw_port )
            CEPH_RGW_PORT=$2
            shift
            ;;
    mon )
	    start_mon=1
	    start_all=0
	    ;;
    mds )
	    start_mds=1
	    start_all=0
	    ;;
    osd )
	    start_osd=1
	    start_all=0
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
	    memstore=1
	    ;;
    --bluestore )
	    bluestore=1
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
    * )
	    usage_exit
esac
shift
done

if [ "$overwrite_conf" -eq 0 ]; then
    MON=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC num_mon 2>/dev/null` && \
        CEPH_NUM_MON="$MON"
    OSD=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC num_osd 2>/dev/null` && \
        CEPH_NUM_OSD="$OSD"
    MDS=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC num_mds 2>/dev/null` && \
        CEPH_NUM_MDS="$MDS"
    RGW=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC num_rgw 2>/dev/null` && \
        CEPH_NUM_RGW="$RGW"
fi

if [ "$start_all" -eq 1 ]; then
	start_mon=1
	start_mds=1
	start_osd=1
fi

ARGS="-c $conf_fn"

run() {
    type=$1
    shift
    eval "valg=\$valgrind_$type"
    [ -z "$valg" ] && valg="$valgrind"

    if [ -n "$valg" ]; then
	echo "valgrind --tool=$valg $* -f &"
	valgrind --tool=$valg $* -f &
	sleep 1
    else
	if [ "$nodaemon" -eq 0 ]; then
	    echo "$*"
	    $*
	else
	    echo "ceph-run $* -f &"
	    ./ceph-run $* -f &
	fi
    fi
}

if [ "$debug" -eq 0 ]; then
    CMONDEBUG='
	debug mon = 10
        debug ms = 1'
    COSDDEBUG='
        debug ms = 1'
    CMDSDEBUG='
        debug ms = 1'
else
    echo "** going verbose **"
    CMONDEBUG='
	debug mon = 20
        debug paxos = 20
        debug auth = 20
        debug ms = 1'
    COSDDEBUG='
        debug ms = 1
        debug osd = 25
        debug objecter = 20
        debug monc = 20
        debug journal = 20
        debug filestore = 20
        debug bluestore = 30
        debug bluefs = 20
        debug rocksdb = 10
        debug bdev = 20
        debug rgw = 20
        debug objclass = 20'
    CMDSDEBUG='
        debug ms = 1
        debug mds = 20
        debug auth = 20
        debug monc = 20
        mds debug scatterstat = true
        mds verify scatter = true
        mds log max segments = 2'
fi

if [ -n "$MON_ADDR" ]; then
	CMON_ARGS=" -m "$MON_ADDR
	COSD_ARGS=" -m "$MON_ADDR
	CMDS_ARGS=" -m "$MON_ADDR
fi

if [ "$memstore" -eq 1 ]; then
    COSDMEMSTORE='
	osd objectstore = memstore'
fi
if [ "$bluestore" -eq 1 ]; then
    COSDMEMSTORE='
	osd objectstore = bluestore'
fi

# lockdep everywhere?
# export CEPH_ARGS="--lockdep 1"

if [ -z "$CEPH_PORT" ]; then
    CEPH_PORT=6789
    [ -e ".ceph_port" ] && CEPH_PORT=`cat .ceph_port`
fi

[ -z "$INIT_CEPH" ] && INIT_CEPH=$CEPH_BIN/init-ceph

# sudo if btrfs
test -d $CEPH_DEV_DIR/osd0/. && test -e $CEPH_DEV_DIR/sudo && SUDO="sudo"

if [ "$start_all" -eq 1 ]; then
    $SUDO $INIT_CEPH stop
fi
$SUDO rm -f core*

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
    # filter out IPv6 and localhost addresses
    IP="$(ifconfig | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p' | head -n1)"
    # if nothing left, try using localhost address, it might work
    if [ -z "$IP" ]; then IP="127.0.0.1"; fi
    echo ip $IP
fi
echo "ip $IP"
echo "port $PORT"


[ -z $CEPH_ADM ] && CEPH_ADM=$CEPH_BIN/ceph

if [ "$cephx" -eq 1 ]; then
    CEPH_ADM="$CEPH_ADM -c $conf_fn -k $keyring_fn"
else
    CEPH_ADM="$CEPH_ADM -c $conf_fn"
fi

MONS=""
count=0
for f in a b c d e f g h i j k l m n o p q r s t u v w x y z
do
    if [ -z "$MONS" ];
    then
	MONS="$f"
    else
	MONS="$MONS $f"
    fi
    count=$(($count + 1))
    [ $count -eq $CEPH_NUM_MON ] && break;
done

DAEMONOPTS="
	log file = $CEPH_OUT_DIR/\$name.log
        admin socket = $CEPH_OUT_DIR/\$name.asok
	chdir = \"\"
	pid file = $CEPH_OUT_DIR/\$name.pid
        heartbeat file = $CEPH_OUT_DIR/\$name.heartbeat
"


if [ "$start_mon" -eq 1 ]; then

	if [ "$new" -eq 1 ]; then
		if [ $overwrite_conf -eq 1 ]; then
		        cat <<EOF > $conf_fn
; generated by vstart.sh on `date`
[$VSTART_SEC]
        num mon = $CEPH_NUM_MON
        num osd = $CEPH_NUM_OSD
        num mds = $CEPH_NUM_MDS
        num rgw = $CEPH_NUM_RGW

[global]
        fsid = $(uuidgen)
        osd pg bits = 3
        osd pgp bits = 5  ; (invalid, but ceph should cope!)
        osd crush chooseleaf type = 0
        osd pool default min size = 1
        osd failsafe full ratio = .99
        mon osd reporter subtree level = osd
        mon osd full ratio = .99
        mon data avail warn = 10
        mon data avail crit = 1
        erasure code dir = $EC_PATH
        plugin dir = $CS_PATH
        osd pool default erasure code profile = plugin=jerasure technique=reed_sol_van k=2 m=1 ruleset-failure-domain=osd
        rgw frontends = fastcgi, civetweb port=$CEPH_RGW_PORT
        rgw dns name = localhost
        filestore fd cache size = 32
        run dir = $CEPH_OUT_DIR
        enable experimental unrecoverable data corrupting features = *
        lockdep = true
EOF
if [ "$cephx" -eq 1 ] ; then
cat <<EOF >> $conf_fn
        auth supported = cephx
EOF
else
cat <<EOF >> $conf_fn
	auth cluster required = none
	auth service required = none
	auth client required = none
EOF
fi
			cat <<EOF >> $conf_fn

[client]
        keyring = $keyring_fn
        log file = $CEPH_OUT_DIR/\$name.\$pid.log
        admin socket = $CEPH_OUT_DIR/\$name.\$pid.asok

[mds]
$DAEMONOPTS
$CMDSDEBUG
        mds debug frag = true
        mds debug auth pins = true
        mds debug subtrees = true
        mds data = $CEPH_DEV_DIR/mds.\$id
        mds root ino uid = `id -u`
        mds root ino gid = `id -g`
$extra_conf
[osd]
$DAEMONOPTS
        osd data = $CEPH_DEV_DIR/osd\$id
        osd journal = $CEPH_DEV_DIR/osd\$id/journal
        osd journal size = 100
        osd class tmp = out
        osd class dir = $OBJCLASS_PATH
        osd scrub load threshold = 2000.0
        osd debug op order = true
        filestore wbthrottle xfs ios start flusher = 10
        filestore wbthrottle xfs ios hard limit = 20
        filestore wbthrottle xfs inodes hard limit = 30
        filestore wbthrottle btrfs ios start flusher = 10
        filestore wbthrottle btrfs ios hard limit = 20
        filestore wbthrottle btrfs inodes hard limit = 30
	bluestore fsck on mount = true
	bluestore block create = true
	bluestore block db size = 67108864
	bluestore block db create = true
	bluestore block wal size = 134217728
	bluestore block wal create = true
$COSDDEBUG
$COSDMEMSTORE
$extra_conf
[mon]
        mon pg warn min per osd = 3
        mon osd allow primary affinity = true
        mon reweight min pgs per osd = 4
        mon osd prime pg temp = true
        crushtool = $CEPH_BIN/crushtool
$DAEMONOPTS
$CMONDEBUG
$extra_conf
        mon cluster log file = $CEPH_OUT_DIR/cluster.mon.\$id.log
[global]
$extra_conf
EOF
		fi

		if [ `echo $IP | grep '^127\\.'` ]
		then
			echo
			echo "NOTE: hostname resolves to loopback; remote hosts will not be able to"
			echo "  connect.  either adjust /etc/hosts, or edit this script to use your"
			echo "  machine's real IP."
			echo
		fi

	        $SUDO $CEPH_BIN/ceph-authtool --create-keyring --gen-key --name=mon. $keyring_fn --cap mon 'allow *'
	        $SUDO $CEPH_BIN/ceph-authtool --gen-key --name=client.admin --set-uid=0 \
		    --cap mon 'allow *' \
		    --cap osd 'allow *' \
		    --cap mds 'allow *' \
		    $keyring_fn

		# build a fresh fs monmap, mon fs
		str="$CEPH_BIN/monmaptool --create --clobber"
		count=0
		for f in $MONS
		do
			str=$str" --add $f $IP:$(($CEPH_PORT+$count))"
			if [ $overwrite_conf -eq 1 ]; then
				cat <<EOF >> $conf_fn
[mon.$f]
        host = $HOSTNAME
        mon data = $CEPH_DEV_DIR/mon.$f
        mon addr = $IP:$(($CEPH_PORT+$count))
EOF
			fi
			count=$(($count + 1))
		done
		str=$str" --print $monmap_fn"
		echo $str
		$str

		for f in $MONS
		do
		    cmd="rm -rf $CEPH_DEV_DIR/mon.$f"
		    echo $cmd
		    $cmd
                    cmd="mkdir -p $CEPH_DEV_DIR/mon.$f"
                    echo $cmd
                    $cmd
		    cmd="$CEPH_BIN/ceph-mon --mkfs -c $conf_fn -i $f --monmap=$monmap_fn"
		    cmd="$cmd --keyring=$keyring_fn"
		    echo $cmd
		    $cmd
		done

		rm $monmap_fn
	fi

	# start monitors
	if [ "$start_mon" -ne 0 ]; then
		for f in $MONS
		do
		    run 'mon' $CEPH_BIN/ceph-mon -i $f $ARGS $CMON_ARGS
		done
	fi
fi

#osd
if [ "$start_osd" -eq 1 ]; then
    for osd in `seq 0 $((CEPH_NUM_OSD-1))`
    do
	if [ "$new" -eq 1 ]; then
	    if [ $overwrite_conf -eq 1 ]; then
		    cat <<EOF >> $conf_fn
[osd.$osd]
        host = $HOSTNAME
EOF
	    fi

	    rm -rf $CEPH_DEV_DIR/osd$osd || true
	    for f in $CEPH_DEV_DIR/osd$osd/*; do btrfs sub delete $f &> /dev/null || true; done
	    mkdir -p $CEPH_DEV_DIR/osd$osd

	    uuid=`uuidgen`
	    echo "add osd$osd $uuid"
	    $SUDO $CEPH_ADM osd create $uuid
	    $SUDO $CEPH_ADM osd crush add osd.$osd 1.0 host=$HOSTNAME root=default
	    $SUDO $CEPH_BIN/ceph-osd -i $osd $ARGS --mkfs --mkkey --osd-uuid $uuid

	    key_fn=$CEPH_DEV_DIR/osd$osd/keyring
	    echo adding osd$osd key to auth repository
	    $SUDO $CEPH_ADM -i $key_fn auth add osd.$osd osd "allow *" mon "allow profile osd"
	fi
	echo start osd$osd
	run 'osd' $SUDO $CEPH_BIN/ceph-osd -i $osd $ARGS $COSD_ARGS
    done
fi

# mds
if [ "$smallmds" -eq 1 ]; then
    cat <<EOF >> $conf_fn
[mds]
	mds log max segments = 2
	mds cache size = 10000
EOF
fi

if [ "$start_mds" -eq 1 -a "$CEPH_NUM_MDS" -gt 0 ]; then
    if [ "$CEPH_NUM_FS" -gt "1" ] ; then
        $CEPH_ADM fs flag set enable_multiple true
    fi

    fs=0
    for name in a b c d e f g h i j k l m n o p
    do
        cmd="$CEPH_ADM osd pool create cephfs_data_${name} 8"
        $cmd

        cmd="$CEPH_ADM osd pool create cephfs_metadata_${name} 8"
        $cmd

        cmd="$CEPH_ADM fs new cephfs_${name} cephfs_metadata_${name} cephfs_data_${name}"
        $cmd
        fs=$(($fs + 1))
        [ $fs -eq $CEPH_NUM_FS ] && break
    done

    mds=0
    for name in a b c d e f g h i j k l m n o p
    do
	if [ "$new" -eq 1 ]; then
	    mkdir -p $CEPH_DEV_DIR/mds.$name
	    key_fn=$CEPH_DEV_DIR/mds.$name/keyring
	    if [ $overwrite_conf -eq 1 ]; then
	    cat <<EOF >> $conf_fn
[mds.$name]
        host = $HOSTNAME
EOF
		if [ "$standby" -eq 1 ]; then
		    mkdir -p $CEPH_DEV_DIR/mds.${name}s
		    cat <<EOF >> $conf_fn
       mds standby for rank = $mds
[mds.${name}s]
        mds standby replay = true
        mds standby for name = ${name}
EOF
		fi
	    fi
	    $SUDO $CEPH_BIN/ceph-authtool --create-keyring --gen-key --name=mds.$name $key_fn
	    $SUDO $CEPH_ADM -i $key_fn auth add mds.$name mon 'allow profile mds' osd 'allow *' mds 'allow'
	    if [ "$standby" -eq 1 ]; then
		    $SUDO $CEPH_BIN/ceph-authtool --create-keyring --gen-key --name=mds.${name}s \
			$CEPH_DEV_DIR/mds.${name}s/keyring
                    $SUDO $CEPH_ADM -i $CEPH_DEV_DIR/mds.${name}s/keyring auth add mds.${name}s \
			mon 'allow *' osd 'allow *' mds 'allow'
	    fi

	fi
	
	run 'mds' $CEPH_BIN/ceph-mds -i $name $ARGS $CMDS_ARGS
	if [ "$standby" -eq 1 ]; then
	    run 'mds' $CEPH_BIN/ceph-mds -i ${name}s $ARGS $CMDS_ARGS
	fi
	
	mds=$(($mds + 1))
	[ $mds -eq $CEPH_NUM_MDS ] && break

#valgrind --tool=massif $CEPH_BIN/ceph-mds $ARGS --mds_log_max_segments 2 --mds_thrash_fragments 0 --mds_thrash_exports 0 > m  #--debug_ms 20
#$CEPH_BIN/ceph-mds -d $ARGS --mds_thrash_fragments 0 --mds_thrash_exports 0 #--debug_ms 20
#$CEPH_ADM mds set max_mds 2
    done
fi

if [ "$ec" -eq 1 ]; then
    $SUDO $CEPH_ADM <<EOF
osd erasure-code-profile set ec-profile m=2 k=2
osd pool create ec 8 8 erasure ec-profile
EOF
fi

do_cache() {
    while [ -n "$*" ]; do
	p="$1"
	shift
	echo "creating cache for pool $p ..."
	$SUDO $CEPH_ADM <<EOF
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
	$CEPH_ADM <<EOF
osd pool set $pool hit_set_type $type
osd pool set $pool hit_set_count 8
osd pool set $pool hit_set_period 30
EOF
    done
}
do_hitsets $hitset

do_rgw()
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

    # Start server
    echo start rgw on http://localhost:$CEPH_RGW_PORT
    RGWDEBUG=""
    if [ "$debug" -ne 0 ]; then
        RGWDEBUG="--debug-rgw=20"
    fi

    RGWSUDO=
    [ $CEPH_RGW_PORT -lt 1024 ] && RGWSUDO=sudo
    $RGWSUDO $CEPH_BIN/radosgw -c $conf_fn --log-file=${CEPH_OUT_DIR}/rgw.log ${RGWDEBUG} --debug-ms=1
}
if [ "$start_rgw" -eq 1 ]; then
    do_rgw
fi

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

echo ""
echo "export PYTHONPATH=./pybind:$PYTHONPATH"
echo "export LD_LIBRARY_PATH=$CEPH_LIB"

if [ "$CEPH_DIR" != "$PWD" ]; then
    echo "export CEPH_CONF=$conf_fn"
    echo "export CEPH_KEYRING=$keyring_fn"
fi
