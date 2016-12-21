#!/bin/sh
#
# Make Alpine Packages
#

set -xe

#
# make a distribution
#
./make-dist
mv -f *.tar.bz2 ./alpine

#
# alpine packaging key stuff
#
rm -rf .abuild && mkdir -p .abuild
ABUILD_USERDIR=$(pwd)/.abuild abuild-keygen -n -a
source .abuild/abuild.conf

#
# package it
#
cd alpine
abuild checksum && JOBS=$(expr $(nproc) / 2) SRCDEST=$(pwd) REPODEST=$(pwd) PACKAGER_PRIVKEY=$PACKAGER_PRIVKEY abuild -r
cd ..
