#!/bin/sh

./stop.sh
rm core*

test -d out || mkdir out
rm out/*

# figure machine's ip
HOSTNAME=`hostname`
IP=`host $HOSTNAME | grep $HOSTNAME | cut -d ' ' -f 4`
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
$CEPH_BIN/monmaptool --create --clobber --add $IP:12345 --add $IP:12346 --add $IP:12347 --print .ceph_monmap
for f in 0 1 2
do
 $CEPH_BIN/mkmonfs --clobber mondata/mon$f --mon $f --monmap .ceph_monmap
 $CEPH_BIN/cmon -d mondata/mon$f --debug_mon 20 --debug_ms 1 --debug_paxos 10
done

# shared args
ARGS="-d"

# build and inject an initial osd map
$CEPH_BIN/osdmaptool --clobber --createsimple .ceph_monmap 4 --print .ceph_osdmap # --pgbits 2
$CEPH_BIN/cmonctl osd setmap 2 -i .ceph_osdmap

for osd in 0 #1 2 3 #4 5 6 7 8 9 10 11 12 13 14 15
do
 $CEPH_BIN/cosd --mkfs_for_osd $osd dev/osd$osd  # initialize empty object store
 #valgrind --leak-check=full --show-reachable=yes $CEPH_BIN/cosd dev/osd$osd --debug_ms 1 --debug_osd 20 --debug_filestore 10 1>out/o$osd & #--debug_osd 40
 $CEPH_BIN/cosd dev/osd$osd -d --debug_ms 1 --debug_osd 20 # --debug_filestore 10
done

# mds
$CEPH_BIN/cmds $ARGS --debug_ms 1 --debug_mds 20 --mds_log_max_segments 10 --mds_thrash_fragments 0 --mds_thrash_exports 0 #--debug_ms 20
#$CEPH_BIN/cmds $ARGS --debug_ms 1 --debug_mds 20 --mds_thrash_fragments 0 --mds_thrash_exports 0 #--debug_ms 20
#./cmonctl mds set_max_mds 2

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

