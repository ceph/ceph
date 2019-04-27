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
set -xe

base=${1:-/tmp/release}
codename=$(lsb_release -sc)
releasedir=$base/$(lsb_release -si)/WORKDIR
rm -fr $(dirname $releasedir)
mkdir -p $releasedir
#
# remove all files not under git so they are not
# included in the distribution.
#
git clean -dxf
#
# git describe provides a version that is
# a) human readable
# b) is unique for each commit
# c) compares higher than any previous commit
# d) contains the short hash of the commit
#
vers=$(git describe --match "v*" | sed s/^v//)
./make-dist $vers
#
# rename the tarbal to match debian conventions and extract it
#
mv ceph-$vers.tar.bz2 $releasedir/ceph_$vers.orig.tar.bz2
tar -C $releasedir -jxf $releasedir/ceph_$vers.orig.tar.bz2
#
# copy the debian directory over and remove -dbg packages
# because they are large and take time to build
#
cp -a debian $releasedir/ceph-$vers/debian
cd $releasedir
perl -ni -e 'print if(!(/^Package: .*-dbg$/../^$/))' ceph-$vers/debian/control
perl -pi -e 's/--dbg-package.*//' ceph-$vers/debian/rules
#
# always set the debian version to 1 which is ok because the debian
# directory is included in the sources and the upstream version will
# change each time it is modified.
#
dvers="$vers-1"
#
# update the changelog to match the desired version
#
cd ceph-$vers
chvers=$(head -1 debian/changelog | perl -ne 's/.*\(//; s/\).*//; print')
if [ "$chvers" != "$dvers" ]; then
   DEBEMAIL="contact@ceph.com" dch -D $codename --force-distribution -b -v "$dvers" "new version"
fi
#
# create the packages
# a) with ccache to speed things up when building repeatedly
# b) do not sign the packages
# c) use half of the available processors
#
: ${NPROC:=$(($(nproc) / 2))}
if test $NPROC -gt 1 ; then
    j=-j${NPROC}
fi
PATH=/usr/lib/ccache:$PATH dpkg-buildpackage $j -uc -us
cd ../..
mkdir -p $codename/conf
cat > $codename/conf/distributions <<EOF
Codename: $codename
Suite: stable
Components: main
Architectures: $(dpkg --print-architecture) source
EOF
if [ ! -e conf ]; then
    ln -s $codename/conf conf
fi
reprepro --basedir $(pwd) include $codename WORKDIR/*.changes
#
# teuthology needs the version in the version file
#
echo $dvers > $codename/version
