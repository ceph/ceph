#!/bin/bash

basedir=`echo $0 | sed 's/[^/]*$//g'`.
. $basedir/common.sh

mount
enter_mydir

wget http://tuxera.com/sw/qa/pjd-fstest-20080816.tgz
tar zxvf pjd*
cd pjd*
make
cd ..
mkdir tmp
cd tmp
prove -r ../pjd*/tests

leave_mydir
umount
