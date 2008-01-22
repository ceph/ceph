#!/bin/sh
rm core*
./stop.sh
./mkmonmap `host \`hostname -f\`|cut -d ' ' -f 4`:12345  # your IP here; any unused port will do
./cmon --mkfs --mon 0 &
./cosd --mkfs --osd 0 &
./cosd --mkfs --osd 1 &
./cosd --mkfs --osd 2 &
./cosd --mkfs --osd 3 &
./cmds &
