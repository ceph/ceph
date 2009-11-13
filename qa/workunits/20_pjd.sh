#!/bin/bash
wget http://tuxera.com/sw/qa/pjd-fstest-20080816.tgz
tar zxvf pjd*
cd pjd*
make
cd ..
mkdir tmp
cd tmp
prove -r ../pjd*/tests
