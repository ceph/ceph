#!/bin/sh

rm core*
./stop.sh

test -d out || mkdir out
rm out/*

ARGS="--doutdir out"
./mkmonmap `host \`hostname -f\`|cut -d ' ' -f 4`:12345  # your IP here
./cmon --mkfs --mon 0 $ARGS &
./cosd --mkfs --osd 0 $ARGS &
./cosd --mkfs --osd 1 $ARGS &
./cosd --mkfs --osd 2 $ARGS &
./cosd --mkfs --osd 3 $ARGS &
./cmds $ARGS
echo "started.  stop.sh to stop.  see out/* for debug output."
