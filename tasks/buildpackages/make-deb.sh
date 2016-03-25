#!/bin/bash
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

# 
# Create and upload a deb repository with the same naming conventions
# as https://github.com/ceph/autobuild-ceph/blob/master/build-ceph-deb.sh
#
set -xe

base=/tmp/release
gitbuilder_host=$1
codename=$2
git_ceph_url=$3
sha1=$4
flavor=$5
arch=$6

sudo apt-get update
sudo apt-get install -y git

source $(dirname $0)/common.sh

init_ceph $git_ceph_url $sha1

#codename=$(lsb_release -sc)
releasedir=$base/$(lsb_release -si)/WORKDIR
#
# git describe provides a version that is
# a) human readable
# b) is unique for each commit
# c) compares higher than any previous commit
# d) contains the short hash of the commit
#
vers=$(git describe --match "v*" | sed s/^v//)
#
# always set the debian version to 1 which is ok because the debian
# directory is included in the sources and the upstream version will
# change each time it is modified.
#
dvers="$vers-1"
: ${NPROC:=$(nproc)}
ceph_dir=$(pwd)

function build_package() {

    rm -fr $releasedir
    mkdir -p $releasedir
    #
    # remove all files not under git so they are not
    # included in the distribution.
    #
    git clean -qdxff
    #
    # creating the distribution tarbal requires some configure
    # options (otherwise parts of the source tree will be left out).
    #
    ./autogen.sh
    ./configure $(flavor2configure $flavor) \
        --with-rocksdb --with-ocf \
        --with-nss --with-debug --enable-cephfs-java \
        --with-lttng --with-babeltrace
    #
    # use distdir= to set the name of the top level directory of the
    # tarbal to match the desired version
    #
    make distdir=ceph-$vers dist
    #
    # rename the tarbal to match debian conventions and extract it
    #
    mv ceph-$vers.tar.gz $releasedir/ceph_$vers.orig.tar.gz
    tar -C $releasedir -zxf $releasedir/ceph_$vers.orig.tar.gz
    #
    # copy the debian directory over
    #
    cp -a debian $releasedir/ceph-$vers/debian
    cd $releasedir
    #
    # uncomment to remove -dbg packages
    # because they are large and take time to build
    #
    #perl -ni -e 'print if(!(/^Package: .*-dbg$/../^$/))' ceph-$vers/debian/control
    #perl -pi -e 's/--dbg-package.*//' ceph-$vers/debian/rules
    #
    # update the changelog to match the desired version
    #
    cd ceph-$vers
    local chvers=$(head -1 debian/changelog | perl -ne 's/.*\(//; s/\).*//; print')
    if [ "$chvers" != "$dvers" ]; then
        DEBEMAIL="contact@ceph.com" dch -D $codename --force-distribution -b -v "$dvers" "new version"
    fi
    #
    # create the packages (with ccache)
    #
    j=$(maybe_parallel $NPROC $vers)
    PATH=/usr/lib/ccache:$PATH dpkg-buildpackage $j -uc -us -sa
}

function build_repo() {
    local gitbuilder_host=$1

    sudo apt-get install -y reprepro
    cd ${releasedir}/..
    #
    # Create a repository in a directory with a name structured
    # as
    #
    base=ceph-deb-$codename-$arch-$flavor
    sha1_dir=$codename/$base/sha1/$sha1
    mkdir -p $sha1_dir/conf
    cat > $sha1_dir/conf/distributions <<EOF
Codename: $codename
Suite: stable
Components: main
Architectures: i386 amd64 source
EOF
    reprepro --basedir $sha1_dir include $codename WORKDIR/*.changes
    echo $dvers > $sha1_dir/version
    echo $sha1 > $sha1_dir/sha1
    link_same $codename/$base/ref $ceph_dir $sha1
    if test "$gitbuilder_host" ; then
        cd $codename
        RSYNC_RSH='ssh -o StrictHostKeyChecking=false' rsync -av $base/ $gitbuilder_host:/usr/share/nginx/html/$base/
    fi
}

build_package
build_repo $gitbuilder_host
