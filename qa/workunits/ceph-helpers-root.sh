#!/usr/bin/env bash
#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

#######################################################################

function distro_id() {
    source /etc/os-release
    echo $ID
}

function distro_version() {
    source /etc/os-release
    echo $VERSION
}

function install() {
    if [ $(distro_id) = "ubuntu" ]; then
        sudo apt-get purge -y gcc
        sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
    fi
    for package in "$@" ; do
        install_one $package
    done
    if [ $(distro_id) = "ubuntu" ]; then
        sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 11
        sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 11
        sudo update-alternatives --install /usr/bin/cc cc /usr/bin/gcc 11
        sudo update-alternatives --set cc /usr/bin/gcc
        sudo update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++ 11
        sudo update-alternatives --set c++ /usr/bin/g++
    fi
}

function install_one() {
    case $(distro_id) in
        ubuntu|debian|devuan|softiron)
            sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
            ;;
        centos|fedora|rhel)
            sudo yum install -y "$@"
            ;;
        opensuse*|suse|sles)
            sudo zypper --non-interactive install "$@"
            ;;
        *)
            echo "$(distro_id) is unknown, $@ will have to be installed manually."
            ;;
    esac
}

function install_pkg_on_ubuntu {
    local project=$1
    shift
    local sha1=$1
    shift
    local codename=$1
    shift
    local force=$1
    shift
    local pkgs=$@
    local missing_pkgs
    if [ $force = "force" ]; then
	missing_pkgs="$@"
    else
	for pkg in $pkgs; do
	    if ! dpkg -s $pkg &> /dev/null; then
		missing_pkgs+=" $pkg"
	    fi
	done
    fi
    if test -n "$missing_pkgs"; then
	local shaman_url="https://shaman.ceph.com/api/repos/${project}/master/${sha1}/ubuntu/${codename}/repo"
	sudo curl --silent --location $shaman_url --output /etc/apt/sources.list.d/$project.list
	sudo env DEBIAN_FRONTEND=noninteractive apt-get update -y -o Acquire::Languages=none -o Acquire::Translation=none || true
	sudo env DEBIAN_FRONTEND=noninteractive apt-get install --allow-unauthenticated -y $missing_pkgs
    fi
}

#######################################################################

function control_osd() {
    local action=$1
    local id=$2

    sudo systemctl $action ceph-osd@$id

    return 0
}

#######################################################################

function pool_read_write() {
    local size=${1:-1}
    local dir=/tmp
    local timeout=360
    local test_pool=test_pool

    ceph osd pool delete $test_pool $test_pool --yes-i-really-really-mean-it || return 1
    ceph osd pool create $test_pool 4 || return 1
    ceph osd pool set $test_pool size $size --yes-i-really-mean-it || return 1
    ceph osd pool set $test_pool min_size $size || return 1
    ceph osd pool application enable $test_pool rados

    echo FOO > $dir/BAR
    timeout $timeout rados --pool $test_pool put BAR $dir/BAR || return 1
    timeout $timeout rados --pool $test_pool get BAR $dir/BAR.copy || return 1
    diff $dir/BAR $dir/BAR.copy || return 1
    ceph osd pool delete $test_pool $test_pool --yes-i-really-really-mean-it || return 1
}

#######################################################################

set -x

"$@"
