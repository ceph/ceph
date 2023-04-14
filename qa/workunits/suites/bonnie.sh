#!/usr/bin/env bash

set -ex

bonnie_bin=`which bonnie++`
[ $? -eq 1 ] && bonnie_bin=/usr/sbin/bonnie++

uid_flags=""
[ "`id -u`" == "0" ] && uid_flags="-u root"

$bonnie_bin $uid_flags -n 100
