#!/bin/sh

killall cmon cmds

for host in `cd dev/hosts ; ls`
do
 ssh root@cosd$host killall cosd \; cd $HOME/ceph/src \; for f in devm/\* \; do umount \$f \; rmmod btrfs \; done
done