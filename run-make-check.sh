#!/bin/bash
#
# Ceph distributed storage system
#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#

#
# Return MAX(1, (number of processors / 2)) by default or NPROC
#
function get_processors() {
    if test -n "$NPROC" ; then
        echo $NPROC
    else
        if test $(nproc) -ge 2 ; then
            expr $(nproc) / 2
        else
            echo 1
        fi
    fi
}

DEFAULT_MAKEOPTS=${DEFAULT_MAKEOPTS:--j$(get_processors)}
BUILD_MAKEOPTS=${BUILD_MAKEOPTS:-$DEFAULT_MAKEOPTS}
CHECK_MAKEOPTS=${CHECK_MAKEOPTS:-$DEFAULT_MAKEOPTS}

function run() {
    local install_cmd
    if test -f /etc/redhat-release ; then
        source /etc/os-release
        if test "$(echo "$VERSION_ID >= 22" | bc)" -ne 0; then
            install_cmd="dnf -y install"
        else
            install_cmd="yum install -y"
        fi
    fi

    type apt-get > /dev/null 2>&1 && install_cmd="apt-get install -y"
    type zypper > /dev/null 2>&1 && install_cmd="zypper --gpg-auto-import-keys --non-interactive install"
    if [ -n "$install_cmd" ]; then
        $DRY_RUN sudo $install_cmd ccache jq
    else
        echo "WARNING: Don't know how to install packages" >&2
    fi

    if test -f ./install-deps.sh ; then
	$DRY_RUN ./install-deps.sh || return 1
    fi
    $DRY_RUN ./do_cmake.sh $@ || return 1
    $DRY_RUN cd build
    $DRY_RUN make $BUILD_MAKEOPTS tests || return 1
    $DRY_RUN ctest $CHECK_MAKEOPTS --output-on-failure || return 1
}

function main() {
    if run "$@" ; then
        rm -fr ${CEPH_BUILD_VIRTUALENV:-/tmp}/*virtualenv*
        echo "cmake check: successful run on $(git rev-parse HEAD)"
        return 0
    else
        rm -fr ${CEPH_BUILD_VIRTUALENV:-/tmp}/*virtualenv*
        return 1
    fi
}

main "$@"
