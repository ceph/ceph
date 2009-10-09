#!/bin/bash

basedir=`echo $0 | sed 's/[^/]*$//g'`
basedir="${basedir}."
. $basedir/common.sh

mount
enter_mydir

tar jxvf /root/linux*
cd linux*
make defconfig
make
cd ..
rm -r linux*

leave_mydir
umount
