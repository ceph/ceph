#!/bin/sh

[ -z "$CEPH_NUM_MON" ] && CEPH_NUM_MON=3
[ -z "$CEPH_NUM_OSD" ] && CEPH_NUM_OSD=1
[ -z "$CEPH_NUM_MDS" ] && CEPH_NUM_MDS=3

extra_conf=""
new=0
standby=0
debug=0
start_all=1
start_mon=0
start_mds=0
start_osd=0
localhost=0
nodaemon=0
smallmds=0
overwrite_conf=1
cephx=0

MON_ADDR=""

conf="ceph.conf"

keyring_fn=".ceph_keyring"
osdmap_fn="/tmp/ceph_osdmap.$$"
monmap_fn="/tmp/ceph_monmap.$$"

usage="usage: $0 [option]... [mon] [mds] [osd]\n"
usage=$usage"options:\n"
usage=$usage"\t-d, --debug\n"
usage=$usage"\t-s, --standby_mds: Generate standby-replay MDS for each active\n"
usage=$usage"\t-n, --new\n"
usage=$usage"\t--valgrind[_{osd,mds,mon}] 'toolname args...'\n"
usage=$usage"\t-m ip:port\t\tspecify monitor address\n"
usage=$usage"\t-k keep old configuration files\n"

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
	    localhost=1
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
    mon | cmon )
	    start_mon=1
	    start_all=0
	    ;;
    mds | cmds )
	    start_mds=1
	    start_all=0
	    ;;
    osd | cosd )
	    start_osd=1
	    start_all=0
	    ;;
    -m )
	    [ -z "$2" ] && usage_exit
	    MON_ADDR=$2
	    shift
	    ;;
    -x )
	    cephx=1
	    ;;
    -k )
	    overwrite_conf=0
	    ;;
    -o )
	    extra_conf="$extra_conf	$2
"
	    shift
	    ;;
    * )
	    usage_exit
esac
shift
done

if [ "$start_all" -eq 1 ]; then
	start_mon=1
	start_mds=1
	start_osd=1
fi

ARGS="-c $conf"

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
	    echo "crun $* -f &"
	    ./crun $* -f &
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
        lockdep = 1
	debug mon = 20
        debug paxos = 20
        debug auth = 20
        debug ms = 1'
    COSDDEBUG='
        lockdep = 1
        debug ms = 1
        debug osd = 25
        debug monc = 20
        debug journal = 20
        debug filestore = 10'
    CMDSDEBUG='
        lockdep = 1
        debug ms = 1
        debug mds = 20
        debug auth = 20
        debug monc = 20
        mds debug scatterstat = true
        mds verify scatter = true
        mds log max segments = 2'
fi

if [ "$standby" -eq 1 ]; then
    echo "standby got set"
    CEPH_NUM_MDS=$(($CEPH_NUM_MDS * 2))
fi

if [ -n "$MON_ADDR" ]; then
	CMON_ARGS=" -m "$MON_ADDR
	COSD_ARGS=" -m "$MON_ADDR
	CMDS_ARGS=" -m "$MON_ADDR
fi


# lockdep everywhere?
# export CEPH_ARGS="--lockdep 1"


# sudo if btrfs
test -d dev/osd0/. && test -e dev/sudo && SUDO="sudo"

if [ "$start_all" -eq 1 ]; then
    $SUDO ./init-ceph stop
fi
$SUDO rm -f core*

test -d out || mkdir out
$SUDO rm -rf out/*
test -d log && rm -f log/*
test -d gmon && $SUDO rm -rf gmon/*

[ "$cephx" -eq 1 ] && test -e $keyring_fn && rm $keyring_fn


# figure machine's ip
if [ "$localhost" -eq 1 ]; then
    IP="127.0.0.1"
else
    HOSTNAME=`hostname`
    echo hostname $HOSTNAME
    RAW_IP=`hostname --ip-address`
    # filter out IPv6 and localhost addresses
    IP="$(echo "$RAW_IP"|tr ' ' '\012'|grep -v :|grep -v '^127\.'|head -n1)"
    # if that left nothing, then try to use the raw thing, it might work
    if [ -z "IP" ]; then IP="$RAW_IP"; fi
    echo ip $IP
fi
echo "ip $IP"

[ -z "$CEPH_BIN" ] && CEPH_BIN=.
[ -z "$CEPH_PORT" ] && CEPH_PORT=6789



if [ "$cephx" -eq 1 ]; then
    CEPH_ADM="$CEPH_BIN/ceph -c $conf -k $keyring_fn"
else
    CEPH_ADM="$CEPH_BIN/ceph -c $conf"
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
	log file = out/\$host
	log per instance = true
	log sym history = 100
        profiling logger = true
	profiling logger dir = log
	chdir = \"\"
	pid file = out/\$name.pid
"


if [ "$start_mon" -eq 1 ]; then
	if [ "$new" -eq 1 ]; then
	# build and inject an initial osd map
		$CEPH_BIN/osdmaptool --clobber --createsimple $CEPH_NUM_OSD $osdmap_fn --pg_bits 2 --pgp_bits 4
	fi

	if [ "$new" -eq 1 ]; then
		if [ $overwrite_conf -eq 1 ]; then
		        cat <<EOF > $conf
; generated by vstart.sh on `date`
[global]
        keyring = .ceph_keyring
$extra_conf
EOF
			[ "$cephx" -eq 1 ] && cat<<EOF >> $conf
        auth supported = cephx
EOF
			cat <<EOF >> $conf
[mds]
$DAEMONOPTS
$CMDSDEBUG
        mds debug frag = true
[osd]
$DAEMONOPTS
        osd class tmp = out
        osd class dir = .libs
        osd scrub load threshold = 5.0
$COSDDEBUG
[mon]
$DAEMONOPTS
$CMONDEBUG

[group everyone]
	addr = 0.0.0.0/0

[mount /]
	allow = %everyone
EOF
		fi

		if [ `echo $IP | grep '^127\\.'` ]
		then
			echo
			echo "WARNING: hostname resolves to loopback; remote hosts will not be able to"
			echo "  connect.  either adjust /etc/hosts, or edit this script to use your"
			echo "  machine's real IP."
			echo
		fi

	        [ "$cephx" -eq 1 ] && $SUDO $CEPH_BIN/cauthtool --create-keyring --gen-key --name=mon. $keyring_fn
	        [ "$cephx" -eq 1 ] && $SUDO $CEPH_BIN/cauthtool --gen-key --name=client.admin --set-uid=0 \
		    --cap mon 'allow *' \
		    --cap osd 'allow *' \
		    --cap mds allow \
		    $keyring_fn

		# build a fresh fs monmap, mon fs
		str="$CEPH_BIN/monmaptool --create --clobber"
		count=0
		for f in $MONS
		do
			str=$str" --add $f $IP:$(($CEPH_PORT+$count))"
			if [ $overwrite_conf -eq 1 ]; then
				cat <<EOF >> $conf
[mon.$f]
        mon data = dev/mon.$f
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
		    cmd="$CEPH_BIN/cmon --mkfs -c $conf -i $f --monmap=$monmap_fn --osdmap=$osdmap_fn"
		    [ "$cephx" -eq 1 ] && cmd="$cmd --keyring=$keyring_fn"
		    echo $cmd
		    $cmd
		done

		rm $monmap_fn
	fi

	# start monitors
	if [ "$start_mon" -ne 0 ]; then
		for f in $MONS
		do
		    run 'mon' $CEPH_BIN/cmon -i $f $ARGS $CMON_ARGS
		done
		sleep 1
	fi
fi

rm $osdmap_fn

#osd
if [ "$start_osd" -eq 1 ]; then
    for osd in `seq 0 $((CEPH_NUM_OSD-1))`
    do
	if [ "$new" -eq 1 ]; then
	    if [ $overwrite_conf -eq 1 ]; then
		    cat <<EOF >> $conf
[osd.$osd]
        osd data = dev/osd$osd
        osd journal = dev/osd$osd/journal
        osd journal size = 100
EOF
		    [ "$cephx" -eq 1 ] && cat <<EOF >> $conf
        keyring = dev/osd$osd/keyring
EOF
	    fi
	    echo mkfs osd$osd
	    cmd="$SUDO $CEPH_BIN/cosd -i $osd $ARGS --mkfs"
	    echo $cmd
	    $cmd

	    if [ "$cephx" -eq 1 ]; then
		key_fn=dev/osd$osd/keyring
		$SUDO $CEPH_BIN/cauthtool --create-keyring --gen-key --name=osd.$osd \
		    --cap mon 'allow *' \
		    --cap osd 'allow *' \
		    $key_fn
		echo adding osd$osd key to auth repository
		$SUDO $CEPH_ADM -i $key_fn auth add osd.$osd
	    fi
	fi
	echo start osd$osd
	run 'osd' $SUDO $CEPH_BIN/cosd -i $osd $ARGS $COSD_ARGS
    done
fi

# mds
if [ "$start_mds" -eq 1 ]; then
    mds=0
    last_mds_name=a
    set_standby=0
    for name in a b c d e f g h i j k l m n o p
    do
	if [ "$new" -eq 1 ]; then
	    key_fn=dev/mds.$name.keyring
	    if [ $overwrite_conf -eq 1 ]; then
	    	cat <<EOF >> $conf
[mds.$name]
EOF
		if [ "$smallmds" -eq 1 ]; then
		    cat <<EOF >> $conf
	mds log max segments = 2
	mds cache size = 10000
EOF
		fi
		if [ "$cephx" -eq 1 ]; then
	    	    cat <<EOF >> $conf
        keyring = $key_fn
EOF
		fi
		if [ "$standby" -eq 1 ]; then
		    echo "noticing standby set"
		    if [ "$set_standby" -eq 1 ]; then
			cat <<EOF >> $conf
        mds standby replay = true
        mds standby for name = $last_mds_nama
EOF
			set_standby=0
		    else
			set_standby=1
		    fi
		fi
	    fi
	    $SUDO $CEPH_BIN/cauthtool --create-keyring --gen-key --name=mds.$name \
		--cap mon 'allow *' \
		--cap osd 'allow *' \
		--cap mds 'allow' \
		$key_fn
	    $SUDO $CEPH_ADM -i $key_fn auth add mds.$name
	fi
	
	run 'mds' $CEPH_BIN/cmds -i $name $ARGS $CMDS_ARGS
	
	mds=$(($mds + 1))
	last_mds_name=$name
	[ $mds -eq $CEPH_NUM_MDS ] && break

#valgrind --tool=massif $CEPH_BIN/cmds $ARGS --mds_log_max_segments 2 --mds_thrash_fragments 0 --mds_thrash_exports 0 > m  #--debug_ms 20
#$CEPH_BIN/cmds -d $ARGS --mds_thrash_fragments 0 --mds_thrash_exports 0 #--debug_ms 20
#$CEPH_ADM mds set_max_mds 2
    done
    cmd="$CEPH_ADM mds set_max_mds $CEPH_NUM_MDS"
    echo $cmd
    $cmd
fi

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

