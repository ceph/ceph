#!/bin/sh

./dstop.sh
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
./monmaptool --create --clobber --add $IP:12345 --print .ceph_monmap
./mkmonfs --clobber mondata/mon0 --mon 0 --monmap .ceph_monmap

# monitor
./cmon -d mondata/mon0 --debug_mon 20 --debug_ms 1

# build and inject an initial osd map
./osdmaptool --clobber --createsimple .ceph_monmap 16 --print .ceph_osdmap # --pgbits 2
./cmonctl osd setmap -i .ceph_osdmap

#ARGS="-m $IP:12345"

for host in `cd dev/hosts ; ls`
do
 ssh cosd$host killall cosd
 for osd in `cd dev/hosts/$host ; ls`
 do
   dev="dev/hosts/$host/$osd"
   echo "---- host $host osd $osd dev $dev ----"
   ls -al $dev
   ssh cosd$host cd ceph/src \; ./cosd --mkfs_for_osd $osd $dev # --osd_auto_weight 1
   ssh cosd$host cd ceph/src \; ./cosd $dev -d --debug_ms 1 --debug_osd 20 --debug_filestore 10
#   ssh cosd$host cd ceph/src \; valgrind --leak-check-full --show-reachable-yes ./cosd $dev --debug_ms 1 --debug_osd 20 --debug_filestore 10 1>out/o$osd \&
 done
done

# mds
./cmds -d --debug_ms 1 --debug_mds 20 --mds_thrash_fragments 0 --mds_thrash_exports 0 #--debug_ms 20

#echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

