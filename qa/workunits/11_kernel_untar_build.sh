#!/bin/bash

tar jxvf /root/linux*
cd linux*
make defconfig
make
cd ..
rm -r linux*
