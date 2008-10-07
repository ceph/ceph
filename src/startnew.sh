#!/bin/sh

# sudo if btrfs
test -d dev/osd0 && SUDO="sudo"

$SUDO ./stop.sh
$SUDO rm core*

test -d out || mkdir out
$SUDO rm out/*

# figure machine's ip
HOSTNAME=`hostname`
IP=`host $HOSTNAME | cut -d ' ' -f 4`
[ "$CEPH_BIN" == "" ] && CEPH_BIN=.
[ "$CEPH_PORT" == "" ] && CEPH_PORT=12345

echo hostname $HOSTNAME
echo "ip $IP"
if [ `echo $IP | grep '^127\\.'` ]
then
	echo
	echo "WARNING: hostname resolves to loopback; remote hosts will not be able to"
	echo "  connect.  either adjust /etc/hosts, or edit this script to use your"
	echo "  machine's real IP."
	echo
fi

# build a fresh fs monmap, mon fs
$CEPH_BIN/monmaptool --create --clobber --add $IP:$CEPH_PORT --print .ceph_monmap
$CEPH_BIN/mkmonfs --clobber mondata/mon0 --mon 0 --monmap .ceph_monmap

# shared args
ARGS="-f"
CMON_ARGS=""
CMDS_ARGS=""
COSD_ARGS=""

# start monitor
$CEPH_BIN/crun $CEPH_BIN/cmon $ARGS $CMON_ARGS mondata/mon0 --debug_mon 10 --debug_ms 1 &


# build and inject an initial osd map
$CEPH_BIN/osdmaptool --clobber --createsimple .ceph_monmap 4 .ceph_osdmap
$CEPH_BIN/cmonctl osd setmap -i .ceph_osdmap

for osd in 0 #1 2 3 
do
 $SUDO $CEPH_BIN/cosd --mkfs_for_osd $osd dev/osd$osd  # initialize empty object store
 $CEPH_BIN/crun $SUDO $CEPH_BIN/cosd $ARGS $COSD_ARGS dev/osd$osd &
done

# mds
$CEPH_BIN/crun $CEPH_BIN/cmds $ARGS $CMDS_ARGS & # --debug_ms 1 #--debug_mds 20 --debug_ms 20

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

