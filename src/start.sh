#!/bin/sh

./stop.sh

test -d out || mkdir out
rm out/*

# figure machine's ip
HOSTNAME=`hostname -f`
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

$CEPH_BIN/monmaptool --create --clobber --add $IP:12345 --print  # your IP here

ARGS="-d --bind $IP --doutdir out --debug_ms 1"
$CEPH_BIN/cmon $ARGS --mkfs --mon 0
$CEPH_BIN/cosd $ARGS --mkfs --osd 0
$CEPH_BIN/cosd $ARGS --mkfs --osd 1
$CEPH_BIN/cosd $ARGS --mkfs --osd 2
$CEPH_BIN/cosd $ARGS --mkfs --osd 3
$CEPH_BIN/cmds $ARGS --debug_mds 10

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

