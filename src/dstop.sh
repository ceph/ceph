#!/bin/sh

killall cmon cmds

for host in `cd dev/hosts ; ls`
do
 ssh cosd$host killall cosd
done