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

. /etc/os-release
base=${1:-/tmp/release}
releasedir=$base/$NAME/WORKDIR
rm -fr $(dirname $releasedir)
#
# remove all files not under git so they are not
# included in the distribution.

[ -e .git ] && git clean -dxf

# git describe provides a version that is
# a) human readable
# b) is unique for each commit
# c) compares higher than any previous commit
# d) contains the short hash of the commit
#
# CI builds compute the version at an earlier stage, via the same method. Since
# git metadata is not part of the source distribution, we take the version as
# an argument to this script.
#
if [ -z "${2}" ]; then
    vers=$(git describe --match "v*" | sed s/^v//)
    dvers=${vers}-1
else
    vers=${2}
    dvers=${vers}-1${VERSION_CODENAME}
fi

test -f "ceph-$vers.tar.bz2" || ./make-dist $vers
#
# rename the tarbal to match debian conventions and extract it
#
mkdir -p $releasedir
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

# For cache hit consistency, allow CI builds to use a build directory whose name
# does not contain version information
if [ "${CEPH_BUILD_NORMALIZE_PATHS}" = 'true' ]; then
    mv ceph-$vers ceph
    cd ceph
else
    cd ceph-$vers
fi

#
# update the changelog to match the desired version
#
chvers=$(head -1 debian/changelog | perl -ne 's/.*\(//; s/\).*//; print')
if [ "$chvers" != "$dvers" ]; then
   DEBEMAIL="contact@ceph.com" dch -D $VERSION_CODENAME --force-distribution -b -v "$dvers" "new version"
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
if [ "$SCCACHE" != "true" ] ; then
    PATH=/usr/lib/ccache:$PATH
fi
PATH=$PATH dpkg-buildpackage $j -uc -us
cd ../..
mkdir -p $VERSION_CODENAME/conf
cat > $VERSION_CODENAME/conf/distributions <<EOF
Codename: $VERSION_CODENAME
Suite: stable
Components: main
Architectures: $(dpkg --print-architecture) source
EOF
if [ ! -e conf ]; then
    ln -s $VERSION_CODENAME/conf conf
fi
reprepro --basedir $(pwd) include $VERSION_CODENAME WORKDIR/*.changes
#
# teuthology needs the version in the version file
#
echo $dvers > $VERSION_CODENAME/version
