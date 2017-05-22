#!/bin/bash

set -ex

#
# SUSE-specific hack to install iozone from OBS (begin)
#
source /etc/os-release
[ "x${ID}x" = "xslesx" ] && [ "x${VERSION_ID}x" = "x12.1x" ] && suse_version="42.1"
[ "x${ID}x" = "xopensusex" ] && [ "x${VERSION_ID}x" = "x42.1x" ] && suse_version="42.1"
[ "x${ID}x" = "xslesx" ] && [ "x${VERSION_ID}x" = "x12.2x" ] && suse_version="42.2"
[ "x${ID}x" = "xopensusex" ] && [ "x${VERSION_ID}x" = "x42.2x" ] && suse_version="42.2"
[ "x${ID}x" = "xslesx" ] && [ "x${VERSION_ID}x" = "x12.3x" ] && suse_version="42.3"
[ "x${ID}x" = "xopensusex" ] && [ "x${VERSION_ID}x" = "x42.3x" ] && suse_version="42.3"
if [ ! -z "$suse_version" ] ; then
    sudo zypper --non-interactive addrepo http://download.opensuse.org/repositories/benchmark/openSUSE_Leap_${suse_version}/benchmark.repo
    sudo zypper --gpg-auto-import-keys ref
    sudo zypper --non-interactive install iozone
fi
#
# SUSE-specific hack to install iozone from OBS (end)
#

iozone -c -e -s 1024M -r 16K -t 1 -F f1 -i 0 -i 1
iozone -c -e -s 1024M -r 1M -t 1 -F f2 -i 0 -i 1
iozone -c -e -s 10240M -r 1M -t 1 -F f3 -i 0 -i 1
