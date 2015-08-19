#!/bin/bash
#
# Ceph distributed storage system
#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
# Modified: Shinobu Kinjo <skinjo@redhat.com>
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#

#
# Return true if the working tree is after the release that made
# make -j8 check possible
#

SUDO=`which sudo`

function can_parallel_make_check() {
    local commit=$(git rev-parse tags/v0.88^{})
    git rev-list HEAD | grep --quiet $commit
}

function maybe_parallel_make_check() {
    if can_parallel_make_check ; then
        echo -j$(get_processors)
    fi
}
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

# We are ready to make ceph
function do_make() {
    $DRY_RUN ./autogen.sh || return 1
    $DRY_RUN ./configure "$@" --disable-static --with-radosgw --with-debug --without-lttng \
        CC="ccache gcc" CXX="ccache g++" CFLAGS="-Wall -g" CXXFLAGS="-Wall -g" || return 1
    $DRY_RUN make -j$(get_processors) || return 1
    $DRY_RUN make $(maybe_parallel_make_check) check || return 1 
    $DRY_RUN make dist || return 1 
}

# All packages will be installed with this function.
function run() {
    # Same logic as install-deps.sh for finding package installer
    local install_cmd

    DIST=`cat /etc/issue`

    if [[ `echo $DIST | grep -i fedora`  ]]; then
        # Fedora?
        $SUDO dnf update -y
        $SUDO dnf install -y dnf-plugins-core redhat-lsb-core ccache jq
    elif [[ `echo $DIST | grep -i centos` ||  `echo $dist | grep -i "red hat"` ]]; then
        # CentOS ro RHEL?
        $SUDO yum update -y
        $SUDO yum install -y yum-utils redhat-lsb-core ccache jq
        #
        MAJOR_VERSION=$(lsb_release -rs | cut -f1 -d.)
        if test $(lsb_release -si) == RedHatEnterpriseServer ; then
            $SUDO yum install subscription-manager
            $SUDO subscription-manager repos --enable=rhel-$MAJOR_VERSION-server-optional-rpms
        fi
        $SUDO yum-config-manager --add-repo https://dl.fedoraproject.org/pub/epel/$MAJOR_VERSION/x86_64/
        $SUDO yum install --nogpgcheck -y epel-release
        $SUDO rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-$MAJOR_VERSION
        $SUDO rm -f /etc/yum.repos.d/dl.fedoraproject.org*
    elif [[ `echo $DIST | grep -i ubuntu` ||  `echo $DIST | egrep -i 'debian|devuan'` ]]; then
        # Ubuntu or Debian?
        $SUDO apt-get update -y
        $SUDO apt-get install -y lsb-release ccache jq dpkg-dev
    elif [[ `echo $DIST | grep -i suse` ]]; then
        # SuSE?
        $SUDO zypper --non-interactive --auto-agree-with-licenses patch
        $SUDO zypper --gpg-auto-import-keys --non-interactive install openSUSE-release lsb-release ccache jq
    else
        # Something -;
        echo "WARNING: Don't know how to install packages" >&2
        echo " Distro: $DIST" >&2
        return 1
    fi

    $SUDO /sbin/modprobe rbd

    if test -f ./install-deps.sh ; then
        $DRY_RUN ./install-deps.sh || return 1
    fi

    do_make "$@"
}

function main() {
    if run "$@" ; then
        echo "make check: successful run on $(git rev-parse HEAD)"
        return 0
    else
        return 1
    fi
}

main "$@"
