#!/bin/bash

killall cmon cmds crun

for host in `cd dev/hosts ; ls`
do
 ssh root@$host killall crun cosd \; cd $HOME/ceph/src/dev/hosts/$host \; for f in \* \; do umount $HOME/ceph/src/devm/osd\$f \; done \; rmmod btrfs
done