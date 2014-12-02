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
DIR=/tmp/install-deps.$$
trap "rm -fr $DIR" EXIT
mkdir -p $DIR

case $(lsb_release -si) in
Ubuntu|Debian|Devuan)
        sudo apt-get install -y dpkg-dev
        touch $DIR/status
        packages=$(dpkg-checkbuilddeps --admindir=$DIR debian/control 2>&1 | \
            perl -p -e 's/.*Unmet build dependencies: *//;' \
            -e 's/build-essential:native/build-essential/;' \
            -e 's/\(.*?\)//g;' \
            -e 's/ +/\n/g;' | sort)
        case $(lsb_release -sc) in
            squeeze)
                packages=$(echo $packages | perl -pe 's/\w*babeltrace\w*//g')
                ;;
        esac
        sudo apt-get install -y $packages
        ;;
CentOS|Fedora|SUSE*|RedHatEnterpriseServer)
        case $(lsb_release -si) in
            SUSE*)
                sudo zypper -y yum-utils
                ;;
            *)
                sudo yum install -y yum-utils
                ;;
        esac
        sed -e 's/@//g' < ceph.spec.in > $DIR/ceph.spec
        sudo yum-builddep -y $DIR/ceph.spec
        ;;
*)
        echo "$(lsb_release -si) is unknown, dependencies will have to be installed manually."
        ;;
esac
