#!/bin/sh
rm core*
./stop.sh
./mkmonmap `host \`hostname -f\`|cut -d ' ' -f 4`:12345  # your IP here
test -d out || mkdir out
rm out/*
./cmon --mkfs --mon 0 --doutdir out &
./cosd --mkfs --osd 0 --doutdir out &
./cosd --mkfs --osd 1 --doutdir out &
./cosd --mkfs --osd 2 --doutdir out &
./cosd --mkfs --osd 3 --doutdir out &
./cmds --doutdir out &
