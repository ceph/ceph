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
# Create and upload a RPM repository with the same naming conventions
# as https://github.com/ceph/autobuild-ceph/blob/master/build-ceph-rpm.sh
#

set -xe

base=/tmp/release
gitbuilder_host=$1
codename=$2
git_ceph_url=$3
sha1=$4
flavor=$5
arch=$6
canonical_tags=$7

suse=false
[[ $codename =~ suse ]] && suse=true
[[ $codename =~ sle ]] && suse=true

if [ "$suse" = true ] ; then
    for delay in 60 60 60 60 ; do
        sudo zypper --non-interactive --no-gpg-checks refresh && break
        sleep $delay
    done
    sudo zypper --non-interactive install --no-recommends git createrepo
else
    sudo yum install -y git createrepo
fi

export BUILDPACKAGES_CANONICAL_TAGS=$canonical_tags
source $(dirname $0)/common.sh

init_ceph $git_ceph_url $sha1

distro=$( source /etc/os-release ; echo $ID )
distro_version=$( source /etc/os-release ; echo $VERSION )
releasedir=$base/$distro/WORKDIR
#
# git describe provides a version that is
# a) human readable
# b) is unique for each commit
# c) compares higher than any previous commit
#    WAIT, c) DOES NOT HOLD:
#    >>> print 'v10.2.5-7-g000000' < 'v10.2.5-8-g000000'
#    True
#    >>> print 'v10.2.5-9-g000000' < 'v10.2.5-10-g000000'
#    False
# d) contains the short hash of the commit
#
# Regardless, we use it for the RPM version number, but strip the leading 'v'
# and replace the '-' before the 'g000000' with a '.' to match the output of
# "rpm -q $PKG --qf %{VERSION}-%{RELEASE}"
#
vers=$(git describe --match "v*" | sed -r -e 's/^v//' -e 's/\-([[:digit:]]+)\-g/\-\1\.g/')
ceph_dir=$(pwd)

#
# Create a repository in a directory with a name structured
# as
#
base=ceph-rpm-$codename-$arch-$flavor

function setup_rpmmacros() {
    if ! grep -q find_debuginfo_dwz_opts $HOME/.rpmmacros ; then
        echo '%_find_debuginfo_dwz_opts %{nil}' >> $HOME/.rpmmacros
    fi
    if [ "x${distro}x" = "xcentosx" ] && echo $distro_version | grep -q '7' ; then
        if ! grep -q '%dist .el7' $HOME/.rpmmacros ; then
            echo '%dist .el7' >> $HOME/.rpmmacros
        fi
    fi
}

function build_package() {
    rm -fr $releasedir
    mkdir -p $releasedir
    #
    # remove all files not under git so they are not
    # included in the distribution.
    #
    git clean -qdxff
    # autotools only works in jewel and below
    if [[ ! -e "make-dist" ]] ; then
        # lsb-release is required by install-deps.sh 
        # which is required by autogen.sh
        if [ "$suse" = true ] ; then
            sudo zypper -n install bzip2 lsb-release which
        else
            sudo yum install -y bzip2 redhat-lsb-core which
        fi
        ./autogen.sh
        #
        # creating the distribution tarball requires some configure
        # options (otherwise parts of the source tree will be left out).
        #
        ./configure $(flavor2configure $flavor) --with-debug --with-radosgw --with-fuse --with-libatomic-ops --with-gtk2 --with-nss

        #
        # use distdir= to set the name of the top level directory of the
        # tarbal to match the desired version
        #
        make dist-bzip2
    else
        # kraken and above
        ./make-dist
    fi
    # Set up build area
    setup_rpmmacros
    if [ "$suse" = true ] ; then
        sudo zypper -n install rpm-build
    else
        sudo yum install -y rpm-build
    fi
    local buildarea=$releasedir
    mkdir -p ${buildarea}/SOURCES
    mkdir -p ${buildarea}/SRPMS
    mkdir -p ${buildarea}/SPECS
    cp ceph.spec ${buildarea}/SPECS
    mkdir -p ${buildarea}/RPMS
    mkdir -p ${buildarea}/BUILD
    CEPH_TARBALL=( ceph-*.tar.bz2 )
    CEPH_TARBALL_BASE=$(echo $CEPH_TARBALL | sed -e 's/.tar.bz2$//')
    CEPH_VERSION=$(echo $CEPH_TARBALL_BASE | cut -d - -f 2-2)
    CEPH_RELEASE=$(echo $CEPH_TARBALL_BASE | cut -d - -f 3- | tr - .)
    cp -a $CEPH_TARBALL ${buildarea}/SOURCES/.
    cp -a rpm/*.patch ${buildarea}/SOURCES || true
    (
        cd ${buildarea}/SPECS
        ccache=$(echo /usr/lib*/ccache)
        if [ "$suse" = true ]; then
          sed -i \
                 -e '0,/%package/s//%debug_package\n\n&/' \
                 -e 's/%bcond_with ceph_test_package/%bcond_without ceph_test_package/g' \
                 -e "s/^Version:.*/Version: $CEPH_VERSION/g" \
                 -e "s/^Release:.*/Release: $CEPH_RELEASE/g" \
                 -e "s/^Source0:.*/Source0: $CEPH_TARBALL/g" \
                 -e '/^Source9/d' \
                 -e "s/^%autosetup -p1.*/%autosetup -p1 -n $CEPH_TARBALL_BASE/g" \
                 ceph.spec
        fi
        cat ceph.spec
        buildarea=`readlink -fn ${releasedir}`   ### rpm wants absolute path
        PATH=$ccache:$PATH rpmbuild -ba --nosignature \
          --define '_srcdefattr (-,root,root)' \
          --define "_unpackaged_files_terminate_build 0" \
          --define "_topdir ${buildarea}" \
          ceph.spec
    )
}

function build_rpm_release() {
    local buildarea=$1
    local sha1=$2
    local gitbuilder_host=$3
    local base=$4

    cat <<EOF > ${buildarea}/SPECS/ceph-release.spec
Name:           ceph-release
Version:        1
Release:        0%{?dist}
Summary:        Ceph repository configuration
Group:          System Environment/Base
License:        GPLv2
URL:            http://gitbuilder.ceph.com/$dist
Source0:        ceph.repo
#Source0:        RPM-GPG-KEY-CEPH
#Source1:        ceph.repo
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:	noarch

%description
This package contains the Ceph repository GPG key as well as configuration
for yum and up2date.

%prep

%setup -q  -c -T
install -pm 644 %{SOURCE0} .
#install -pm 644 %{SOURCE1} .

%build

%install
rm -rf %{buildroot}
#install -Dpm 644 %{SOURCE0} \
#    %{buildroot}/%{_sysconfdir}/pki/rpm-gpg/RPM-GPG-KEY-CEPH
install -dm 755 %{buildroot}/%{_sysconfdir}/yum.repos.d
install -pm 644 %{SOURCE0} \
    %{buildroot}/%{_sysconfdir}/yum.repos.d

%clean
#rm -rf %{buildroot}

%post

%postun

%files
%defattr(-,root,root,-)
#%doc GPL
/etc/yum.repos.d/*
#/etc/pki/rpm-gpg/*

%changelog
* Tue Mar 12 2013 Gary Lowell <glowell@inktank.com> - 1-0
- Handle both yum and zypper
- Use URL to ceph git repo for key
- remove config attribute from repo file
* Tue Aug 28 2012 Gary Lowell <glowell@inktank.com> - 1-0
- Initial Package
EOF

    cat <<EOF > $buildarea/SOURCES/ceph.repo
[Ceph]
name=Ceph packages for \$basearch
baseurl=http://${gitbuilder_host}/${base}/sha1/${sha1}/\$basearch
enabled=1
gpgcheck=0
type=rpm-md

[Ceph-noarch]
name=Ceph noarch packages
baseurl=http://${gitbuilder_host}/${base}/sha1/${sha1}/noarch
enabled=1
gpgcheck=0
type=rpm-md

[ceph-source]
name=Ceph source packages
baseurl=http://${gitbuilder_host}/${base}/sha1/${sha1}/SRPMS
enabled=1
gpgcheck=0
type=rpm-md
EOF

    rpmbuild -bb --define "_topdir ${buildarea}" ${buildarea}/SPECS/ceph-release.spec
}

function build_rpm_repo() {
    local buildarea=$1
    local gitbuilder_host=$2
    local base=$3

    for dir in ${buildarea}/SRPMS ${buildarea}/RPMS/*
    do
        createrepo ${dir}
    done

    local sha1_dir=${buildarea}/../$codename/$base/sha1/$sha1
    mkdir -p $sha1_dir
    echo $vers > $sha1_dir/version
    echo $sha1 > $sha1_dir/sha1
    echo ceph > $sha1_dir/name

    for dir in ${buildarea}/SRPMS ${buildarea}/RPMS/*
    do
        cp -fla ${dir} $sha1_dir
    done

    link_same ${buildarea}/../$codename/$base/ref $ceph_dir $sha1
    if test "$gitbuilder_host" ; then
        (
            cd ${buildarea}/../$codename
            RSYNC_RSH='ssh -o StrictHostKeyChecking=false' rsync -av $base/ ubuntu@$gitbuilder_host:/usr/share/nginx/html/$base/
        )
    fi
}

setup_rpmmacros
build_package
build_rpm_release $releasedir $sha1 $gitbuilder_host $base
build_rpm_repo $releasedir $gitbuilder_host $base
