#!/bin/bash

basedir=`echo $0 | sed 's/[^/]*$//g'`.
. $basedir/common.sh

mount
enter_mydir

dbench 1
dbench 10

leave_mydir
umount
