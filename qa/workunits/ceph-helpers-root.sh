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

function install() {
    for package in "$@" ; do
        install_one $package
    done
}

function install_one() {
    case $(distro_id) in
        ubuntu|debian|devuan)
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

#######################################################################

function control_osd() {
    local action=$1
    local id=$2

    local init=$(ceph-detect-init)

    case $init in
        upstart)
            sudo service ceph-osd $action id=$id
            ;;
        systemd)
            sudo systemctl $action ceph-osd@$id
            ;;
        *)
            echo ceph-detect-init returned an unknown init system: $init >&2
            return 1
            ;;
    esac
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
    ceph osd pool set $test_pool size $size || return 1
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
