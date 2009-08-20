#!/bin/bash

set -e
name=`echo $0 | sed 's/\//_/'`
mkdir $name
cd $name

tar jxvf /root/linux*
cd linux*
make defconfig
make
cd ..
rm -r linux*
