#!/bin/sh

./stop.sh
rm core*

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

# shared args
ARGS="-d --bind $IP -o out --debug_ms 1"

# monitor
$CEPH_BIN/cmon $ARGS mondata/mon0 --debug_mon 20 --debug_ms 1

# osds
for osd in 0 1 2 3 
do
 $CEPH_BIN/cosd $ARGS dev/osd$osd --debug_osd 40
done

# mds
$CEPH_BIN/cmds $ARGS --debug_mds 20

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

