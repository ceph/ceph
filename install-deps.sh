#!/bin/bash -e
#
# Ceph distributed storage system
#
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
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

if [ x`uname`x = xFreeBSDx ]; then
    $SUDO pkg install -yq \
        devel/git \
        devel/gmake \
        devel/cmake \
        devel/yasm \
        devel/boost-all \
        devel/boost-python-libs \
        devel/valgrind \
        devel/pkgconf \
        devel/libatomic_ops \
        devel/libedit \
        devel/libtool \
        devel/google-perftools \
        lang/cython \
        devel/py-virtualenv \
        databases/leveldb \
	net/openldap24-client \
        security/nss \
        security/cryptopp \
        archivers/snappy \
        ftp/curl \
        misc/e2fsprogs-libuuid \
        misc/getopt \
        textproc/expat2 \
        textproc/gsed \
        textproc/libxml2 \
        textproc/xmlstarlet \
	textproc/jq \
	textproc/sphinx \
        emulators/fuse \
        java/junit \
        lang/python27 \
        devel/py-argparse \
        devel/py-nose \
        www/py-flask \
        www/fcgi \
        sysutils/flock \

    exit
else
    source /etc/os-release
    case $ID in
    debian|ubuntu|devuan)
        echo "Using apt-get to install dependencies"
        $SUDO apt-get install -y lsb-release devscripts equivs
        $SUDO apt-get install -y dpkg-dev gcc
        if ! test -r debian/control ; then
            echo debian/control is not a readable file
            exit 1
        fi
        touch $DIR/status

	backports=""
	control="debian/control"
        case $(lsb_release -sc) in
            squeeze|wheezy)
		control="/tmp/control.$$"
		grep -v babeltrace debian/control > $control
                backports="-t $(lsb_release -sc)-backports"
                ;;
        esac

	# make a metapackage that expresses the build dependencies,
	# install it, rm the .deb; then uninstall the package as its
	# work is done
	$SUDO env DEBIAN_FRONTEND=noninteractive mk-build-deps --install --remove --tool="apt-get -y --no-install-recommends $backports" $control || exit 1
	$SUDO env DEBIAN_FRONTEND=noninteractive apt-get -y remove ceph-build-deps
	if [ -n "$backports" ] ; then rm $control; fi
        ;;
    centos|fedora|rhel|ol)
        yumdnf="yum"
        builddepcmd="yum-builddep -y"
        if test "$(echo "$VERSION_ID >= 22" | bc)" -ne 0; then
            yumdnf="dnf"
            builddepcmd="dnf -y builddep --allowerasing"
        fi
        echo "Using $yumdnf to install dependencies"
        $SUDO $yumdnf install -y redhat-lsb-core
        case $(lsb_release -si) in
            Fedora)
                if test $yumdnf = yum; then
                    $SUDO $yumdnf install -y yum-utils
                fi
                ;;
            CentOS|RedHatEnterpriseServer)
                $SUDO yum install -y yum-utils
                MAJOR_VERSION=$(lsb_release -rs | cut -f1 -d.)
                if test $(lsb_release -si) = RedHatEnterpriseServer ; then
                    $SUDO yum install subscription-manager
                    $SUDO subscription-manager repos --enable=rhel-$MAJOR_VERSION-server-optional-rpms
                fi
                $SUDO yum-config-manager --add-repo https://dl.fedoraproject.org/pub/epel/$MAJOR_VERSION/x86_64/
                $SUDO yum install --nogpgcheck -y epel-release
                $SUDO rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-$MAJOR_VERSION
                $SUDO rm -f /etc/yum.repos.d/dl.fedoraproject.org*
                if test $(lsb_release -si) = CentOS -a $MAJOR_VERSION = 7 ; then
                    $SUDO yum-config-manager --enable cr
                fi
                ;;
        esac
        sed -e 's/@//g' < ceph.spec.in > $DIR/ceph.spec
        $SUDO $builddepcmd $DIR/ceph.spec 2>&1 | tee $DIR/yum-builddep.out
        ! grep -q -i error: $DIR/yum-builddep.out || exit 1
        ;;
    opensuse|suse|sles)
        echo "Using zypper to install dependencies"
        $SUDO zypper --gpg-auto-import-keys --non-interactive install lsb-release systemd-rpm-macros
        sed -e 's/@//g' < ceph.spec.in > $DIR/ceph.spec
        $SUDO zypper --non-interactive install $(rpmspec -q --buildrequires $DIR/ceph.spec) || exit 1
        ;;
    *)
        echo "$ID is unknown, dependencies will have to be installed manually."
	exit 1
        ;;
    esac
fi
