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
$CEPH_BIN/monmaptool --create --clobber --add $IP:12345 --print .ceph_monmap
$CEPH_BIN/mkmonfs --clobber mondata/mon0 --mon 0 --monmap .ceph_monmap

# shared args
ARGS="-d"

# start monitor
#valgrind --tool=massif 
#valgrind --leak-check=full --show-reachable=yes $CEPH_BIN/cmon mondata/mon0 --debug_mon 20 --debug_ms 1 > out/mon0 &
#valgrind --tool=massif $CEPH_BIN/cmon mondata/mon0 --debug_mon 20 --debug_ms 1 > out/mon0 &
#sleep 1
$CEPH_BIN/cmon -d mondata/mon0 --debug_mon 20 --debug_ms 1

# build and inject an initial osd map
$CEPH_BIN/osdmaptool --clobber --createsimple .ceph_monmap 4 --print .ceph_osdmap # --pgbits 2
$CEPH_BIN/cmonctl osd setmap -i .ceph_osdmap

for osd in 0 #1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
do
 $CEPH_BIN/cosd --mkfs_for_osd $osd dev/osd$osd  # initialize empty object store
 #valgrind --tool=massif $CEPH_BIN/cosd dev/osd$osd --debug_ms 1 --debug_osd 20 --debug_filestore 10 1>out/o$osd & #--debug_osd 40
 $CEPH_BIN/cosd dev/osd$osd -d --debug_ms 1 --debug_osd 20 --debug_filestore 10
done

# mds
$CEPH_BIN/cmds $ARGS --debug_ms 1 --debug_mds 20 --mds_thrash_fragments 0 --mds_thrash_exports 10 #--debug_ms 20
$CEPH_BIN/cmds $ARGS --debug_ms 1 --debug_mds 20 --mds_thrash_fragments 0 --mds_thrash_exports 10 #--debug_ms 20
./cmonctl mds set_max_mds 2

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

