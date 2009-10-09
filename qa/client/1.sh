#!/bin/bash

basedir=`echo $0 | sed 's/[^/]*$//g'`.
. $basedir/common.sh

mount
enter_mydir

mkdir foo
echo foo > bar
sync

leave_mydir
umount
