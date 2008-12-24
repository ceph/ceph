#!/bin/bash

[ "$CEPH_NUM_MON" == "" ] && CEPH_NUM_MON=3
[ "$CEPH_NUM_OSD" == "" ] && CEPH_NUM_OSD=3
[ "$CEPH_NUM_MDS" == "" ] && CEPH_NUM_MDS=1

let new=0
let debug=0
let start_all=1
let start_mon=0
let start_mds=0
let start_osd=0
norestart=""
valgrind=""
MON_ADDR=""

usage="usage: $0 [option]... [mon] [mds] [osd]\n"
usage=$usage"options:\n"
usage=$usage"\t-d, --debug\n"
usage=$usage"\t-n, --new\n"
usage=$usage"\t--norestart\n"
usage=$usage"\t--valgrind\n"
usage=$usage"\t-m ip:port\t\tspecify monitor address\n"

usage_exit() {
	printf "$usage"
	exit
}

while [ $# -ge 1 ]; do
case $1 in
	-d | --debug )
	debug=1
	;;
	--new | -n )
	new=1
	;;
	--norestart )
	norestart="--norestart"
	;;
	--valgrind )
	valgrind="--valgrind"
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
	[ "$2" == "" ] && usage_exit
	MON_ADDR=$2
	shift
	;;
	* )
	usage_exit
esac
shift
done

if [ $start_all -eq 1 ]; then
	start_mon=1
	start_mds=1
	start_osd=1
fi

ARGS="-f"

if [ $debug -eq 0 ]; then
	CMON_ARGS="--debug_mon 10 --debug_ms 1"
	COSD_ARGS=""
	CMDS_ARGS=""
else
	echo "** going verbose **"
	CMON_ARGS="--lockdep 1 --debug_mon 20 --debug_ms 1 --debug_paxos 20"
	COSD_ARGS="--lockdep 1 --debug_osd 25 --debug_journal 20 --debug_filestore 10 --debug_ms 1" # --debug_journal 20 --debug_osd 20 --debug_filestore 20 --debug_ebofs 20
	CMDS_ARGS="--lockdep 1 --mds_cache_size 500 --mds_log_max_segments 2 --debug_ms 1 --debug_mds 20 --mds_thrash_fragments 0 --mds_thrash_exports 1"
fi

if [ "$MON_ADDR" != "" ]; then
	CMON_ARGS=$CMON_ARGS" -m "$MON_ADDR
	COSD_ARGS=$COSD_ARGS" -m "$MON_ADDR
	CMDS_ARGS=$CMDS_ARGS" -m "$MON_ADDR
fi


# lockdep everywhere?
export CEPH_ARGS="--lockdep 1"


# sudo if btrfs
test -d dev/osd0 && test -e dev/sudo && SUDO="sudo"

if [ $start_all -eq 1 ]; then
	$SUDO ./stop.sh
fi
$SUDO rm -f core*

test -d out || mkdir out
$SUDO rm -f out/*
test -d gmon && $SUDO rm -rf gmon/*


# figure machine's ip
HOSTNAME=`hostname`
IP=`host $HOSTNAME | grep $HOSTNAME | cut -d ' ' -f 4`
[ "$CEPH_BIN" == "" ] && CEPH_BIN=.
[ "$CEPH_PORT" == "" ] && CEPH_PORT=6789
echo hostname $HOSTNAME
echo "ip $IP"

if [ $start_mon -eq 1 ]; then
	if [ $new -eq 1 ]; then
		if [ `echo $IP | grep '^127\\.'` ]
		then
			echo
			echo "WARNING: hostname resolves to loopback; remote hosts will not be able to"
			echo "  connect.  either adjust /etc/hosts, or edit this script to use your"
			echo "  machine's real IP."
			echo
		fi

		# build a fresh fs monmap, mon fs
		# $CEPH_BIN/monmaptool --create --clobber --print .ceph_monmap
		str="$CEPH_BIN/monmaptool --create --clobber"
		for f in `seq 0 $((CEPH_NUM_MON-1))`
		do
			str=$str" --add $IP:$(($CEPH_PORT+$f))"
		done
		str=$str" --print .ceph_monmap"
		echo $str
		$str

		for f in `seq 0 $((CEPH_NUM_MON-1))`
		do
			$CEPH_BIN/mkmonfs --clobber mondata/mon$f --mon $f --monmap .ceph_monmap
		done
	fi

	# start monitors
	if [ $start_mon -ne 0 ]; then
		for f in `seq 0 $((CEPH_NUM_MON-1))`; do
			$CEPH_BIN/cmon $ARGS -d $CMON_ARGS mondata/mon$f
		done
	fi

	if [ $new -eq 1 ]; then
	# build and inject an initial osd map
		$CEPH_BIN/osdmaptool --clobber --createsimple .ceph_monmap 4 .ceph_osdmap # --pgbits 2
		$CEPH_BIN/ceph osd setmap 2 -i .ceph_osdmap 
	fi
fi

#osd
if [ $start_osd -eq 1 ]; then
	for osd in `seq 0 $((CEPH_NUM_OSD-1))`
	do
		if [ $new -eq 1 ]; then
			echo mkfs osd$osd
			$SUDO $CEPH_BIN/cosd --mkfs_for_osd $osd dev/osd$osd # --debug_journal 20 --debug_osd 20 --debug_filestore 20 --debug_ebofs 20
		fi
		echo start osd$osd
		$CEPH_BIN/crun $norestart $valgrind $SUDO $CEPH_BIN/cosd -m $IP:$CEPH_PORT dev/osd$osd $ARGS $COSD_ARGS &
# echo valgrind --leak-check=full --show-reachable=yes $CEPH_BIN/cosd dev/osd$osd --debug_ms 1 --debug_osd 20 --debug_filestore 10 --debug_ebofs 20 #1>out/o$osd #& #--debug_osd 40
	done
fi

# mds
if [ $start_mds -eq 1 ]; then
	for mds in `seq 0 $((CEPH_NUM_MDS-1))`
	do
		$CEPH_BIN/crun $norestart $valgrind $CEPH_BIN/cmds $ARGS $CMDS_ARGS &

#valgrind --tool=massif $CEPH_BIN/cmds $ARGS --mds_log_max_segments 2 --mds_thrash_fragments 0 --mds_thrash_exports 0 > m  #--debug_ms 20
#$CEPH_BIN/cmds -d $ARGS --mds_thrash_fragments 0 --mds_thrash_exports 0 #--debug_ms 20
#$CEPH_BIN/ceph mds set_max_mds 2
	done
	./ceph mds set_max_mds $CEPH_NUM_MDS
fi

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

