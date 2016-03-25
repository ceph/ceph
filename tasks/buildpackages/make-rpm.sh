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

sudo yum install -y git

source $(dirname $0)/common.sh

init_ceph $git_ceph_url $sha1

#id=$(lsb_release -s -i | tr A-Z a-z)
#major=$(lsb_release -s -r | sed -s "s;\..*;;g")
#codename="${id}${major}"
releasedir=$base/$(lsb_release -si)/WORKDIR
#
# git describe provides a version that is
# a) human readable
# b) is unique for each commit
# c) compares higher than any previous commit
# d) contains the short hash of the commit
#
vers=$(git describe --match "v*" | sed s/^v//)
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
    if lsb_release -d -s | grep CentOS | grep -q 'release 7' ; then
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
    #
    # creating the distribution tarbal requires some configure
    # options (otherwise parts of the source tree will be left out).
    #
    sudo yum install -y bzip2
    ./autogen.sh
    ./configure $(flavor2configure $flavor) --with-debug --with-radosgw --with-fuse --with-libatomic-ops --with-gtk2 --with-nss
    #
    # use distdir= to set the name of the top level directory of the
    # tarbal to match the desired version
    #
    make dist-bzip2
    # Set up build area
    setup_rpmmacros
    sudo yum install -y rpm-build
    local buildarea=$releasedir
    mkdir -p ${buildarea}/SOURCES
    mkdir -p ${buildarea}/SRPMS
    mkdir -p ${buildarea}/SPECS
    cp ceph.spec ${buildarea}/SPECS
    mkdir -p ${buildarea}/RPMS
    mkdir -p ${buildarea}/BUILD
    cp -a ceph-*.tar.bz2 ${buildarea}/SOURCES/.
    cp -a rpm/*.patch ${buildarea}/SOURCES || true
    (
        cd ${buildarea}/SPECS
        ccache=$(echo /usr/lib*/ccache)
        # Build RPMs
        buildarea=`readlink -fn ${releasedir}`   ### rpm wants absolute path
        PATH=$ccache:$PATH rpmbuild -ba --define "_unpackaged_files_terminate_build 0" --define "_topdir ${buildarea}" ceph.spec
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
%if 0%{defined suse_version}
install -dm 755 %{buildroot}/%{_sysconfdir}/zypp
install -dm 755 %{buildroot}/%{_sysconfdir}/zypp/repos.d
install -pm 644 %{SOURCE0} \
    %{buildroot}/%{_sysconfdir}/zypp/repos.d
%else
install -dm 755 %{buildroot}/%{_sysconfdir}/yum.repos.d
install -pm 644 %{SOURCE0} \
    %{buildroot}/%{_sysconfdir}/yum.repos.d
%endif

%clean
#rm -rf %{buildroot}

%post

%postun

%files
%defattr(-,root,root,-)
#%doc GPL
%if 0%{defined suse_version}
/etc/zypp/repos.d/*
%else
/etc/yum.repos.d/*
%endif
#/etc/pki/rpm-gpg/*

%changelog
* Tue Mar 10 2013 Gary Lowell <glowell@inktank.com> - 1-0
- Handle both yum and zypper
- Use URL to ceph git repo for key
- remove config attribute from repo file
* Tue Aug 27 2012 Gary Lowell <glowell@inktank.com> - 1-0
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

    sudo yum install -y createrepo

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
