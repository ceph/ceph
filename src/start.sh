#!/bin/sh

./stop.sh

test -d out || mkdir out
rm out/*

# figure machine's ip
HOSTNAME=`hostname -f`
IP=`host $HOSTNAME | cut -d ' ' -f 4`
echo hostname $HOSTNAME
echo "ip $IP"

if [ `echo $IP | grep '^127\\.'` ]
then
	echo
	echo "WARNING: hostname resolves to loopback; remote hosts will not be able to"
	echo "  connect.  either adjust /etc/hsots, or edit this script to use your"
	echo "  machine's real IP."
	echo
fi

ARGS="--bind $IP --doutdir out -d"
./mkmonmap $IP:12345  # your IP here
./cmon --mkfs --mon 0 $ARGS
./cosd --mkfs --osd 0 $ARGS
./cosd --mkfs --osd 1 $ARGS
./cosd --mkfs --osd 2 $ARGS
./cosd --mkfs --osd 3 $ARGS
./cmds $ARGS
echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

