#!/bin/sh

let new=0

while [ $# -ge 1 ]; do
        case $1 in
                --new | -n )
                new=1
        esac
        shift
done

./dstop.sh
rm -f core*

test -d out || mkdir out
rm -f out/*

# mkmonfs
if [ $new -eq 1 ]; then
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
fi

# monitor
./cmon -d mondata/mon0 --debug_mon 20 --debug_ms 1

if [ $new -eq 1 ]; then
    # build and inject an initial osd map
    ./osdmaptool --clobber --createsimple .ceph_monmap 16 --num_dom 4 .ceph_osdmap

    # use custom crush map to separate data from metadata
    ./crushtool -c cm.txt -o cm
    ./osdmaptool --clobber --import-crush cm .ceph_osdmap

    ./cmonctl osd setmap -i .ceph_osdmap
fi


# osds
for host in `cd dev/hosts ; ls`
do
 ssh root@cosd$host killall cosd

 test -d devm && ssh root@cosd$host modprobe crc32c \; insmod $HOME/src/btrfs/kernel/btrfs.ko

 for osd in `cd dev/hosts/$host ; ls`
 do
   dev="dev/hosts/$host/$osd"
   echo "---- host $host osd $osd dev $dev ----"
   devm="$dev"

   # btrfs?
   if [ -d devm ]; then
       devm="devm/osd$osd"
       echo "---- dev mount $devm ----"
       test -d $devm || mkdir -p $devm
       if [ $new -eq 1 ]; then
	   ssh root@cosd$host cd $HOME/ceph/src \; umount $devm \; \
	       $HOME/src/btrfs/progs/mkfs.btrfs $dev \; \
	       mount $dev $devm
       else
	   ssh root@cosd$host cd $HOME/ceph/src \; mount $dev $devm
       fi
   fi

   if [ $new -eq 1 ]; then
       ssh root@cosd$host cd $HOME/ceph/src \; ./cosd --mkfs_for_osd $osd $devm # --osd_auto_weight 1
   fi
   ssh root@cosd$host cd $HOME/ceph/src \; ulimit -c unlimited \; ./cosd $devm -d --debug_ms 1 --debug_osd 10 # --debug_filestore 10 --debug_ebofs 30 --osd_heartbeat_grace 300

 done
done

# mds
./cmds -d --debug_ms 1 --debug_mds 20 --mds_thrash_fragments 0 --mds_thrash_exports 0 #--debug_ms 20


