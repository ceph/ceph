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
if test $(id -u) != 0 ; then
    SUDO=sudo
fi
export LC_ALL=C # the following is vulnerable to i18n

if test -f /etc/redhat-release ; then
    $SUDO yum install -y redhat-lsb-core
fi

if type apt-get > /dev/null 2>&1 ; then
    $SUDO apt-get install -y lsb-release
fi

if type zypper > /dev/null 2>&1 ; then
    $SUDO zypper --gpg-auto-import-keys --non-interactive install openSUSE-release lsb-release
fi

case $(lsb_release -si) in
Ubuntu|Debian|Devuan)
        $SUDO apt-get install -y dpkg-dev
        if ! test -r debian/control ; then
            echo debian/control is not a readable file
            exit 1
        fi
        touch $DIR/status
        packages=$(dpkg-checkbuilddeps --admindir=$DIR debian/control 2>&1 | \
            perl -p -e 's/.*Unmet build dependencies: *//;' \
            -e 's/build-essential:native/build-essential/;' \
            -e 's/\|//g;' \
            -e 's/\(.*?\)//g;' \
            -e 's/ +/\n/g;' | sort)
        case $(lsb_release -sc) in
            squeeze|wheezy)
                packages=$(echo $packages | perl -pe 's/[-\w]*babeltrace[-\w]*//g')
                ;;
        esac
        packages=$(echo $packages) # change newlines into spaces
        $SUDO bash -c "DEBIAN_FRONTEND=noninteractive apt-get install -y $packages"
        ;;
CentOS|Fedora|RedHatEnterpriseServer)
        case $(lsb_release -si) in
            Fedora)
                $SUDO yum install -y yum-utils
                ;;
            CentOS|RedHatEnterpriseServer)
                $SUDO yum install -y yum-utils
                MAJOR_VERSION=$(lsb_release -rs | cut -f1 -d.)
                if test $(lsb_release -si) == RedHatEnterpriseServer ; then
                    $SUDO yum install subscription-manager
                    $SUDO subscription-manager repos --enable=rhel-$MAJOR_VERSION-server-optional-rpms
                fi
                $SUDO yum-config-manager --add-repo https://dl.fedoraproject.org/pub/epel/$MAJOR_VERSION/x86_64/ 
                $SUDO yum install --nogpgcheck -y epel-release
                $SUDO rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-$MAJOR_VERSION
                $SUDO rm -f /etc/yum.repos.d/dl.fedoraproject.org*
                ;;
        esac
        sed -e 's/@//g' < ceph.spec.in > $DIR/ceph.spec
        $SUDO yum-builddep -y $DIR/ceph.spec
        ;;
*SUSE*)
        sed -e 's/@//g' < ceph.spec.in > $DIR/ceph.spec
        $SUDO zypper --non-interactive install $(rpmspec -q --buildrequires $DIR/ceph.spec)
        ;;
*)
        echo "$(lsb_release -si) is unknown, dependencies will have to be installed manually."
        ;;
esac
