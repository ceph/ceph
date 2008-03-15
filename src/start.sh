#!/bin/sh

./stop.sh

test -d out || mkdir out
rm out/*

# figure machine's ip
HOSTNAME=`hostname`
IP=`host $HOSTNAME | cut -d ' ' -f 4`
[ "$CEPH_BIN" == "" ] && CEPH_BIN=.

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
$CEPH_BIN/monmaptool --create --clobber --add $IP:12345 --print .ceph_monmap
$CEPH_BIN/mkmonfs --clobber mondata/mon0 --mon 0 --monmap .ceph_monmap

# shared args
ARGS="-d --bind $IP -o out --debug_ms 1"

# start monitor
$CEPH_BIN/cmon $ARGS mondata/mon0 --debug_mon 20 --debug_ms 1

# build and inject an initial osd map
$CEPH_BIN/osdmaptool --clobber --createsimple .ceph_monmap 4 --print .ceph_osdmap
$CEPH_BIN/cmonctl osd setmap -i .ceph_osdmap

for osd in 0 1 2 3 
do
 $CEPH_BIN/cosd --mkfs_for_osd $osd dev/osd$osd  # initialize empty object store
 $CEPH_BIN/cosd $ARGS dev/osd$osd --debug_osd 10
done

# mds
$CEPH_BIN/cmds $ARGS --debug_mds 10

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

