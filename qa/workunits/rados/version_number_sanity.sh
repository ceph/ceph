#!/bin/bash -ex
#
# test that ceph RPM/DEB package version matches "ceph --version"
# (for a loose definition of "matches")
#
source /etc/os-release
case $ID in
debian|ubuntu)
    RPMDEB='DEB'
    dpkg-query --show ceph-common
    PKG_NAME_AND_VERSION=$(dpkg-query --show ceph-common)
    ;;
centos|fedora|rhel|opensuse*|suse|sles)
    RPMDEB='RPM'
    rpm -q ceph
    PKG_NAME_AND_VERSION=$(rpm -q ceph)
    ;;
*)
    echo "Unsupported distro ->$ID<-! Bailing out."
    exit 1
esac
PKG_CEPH_VERSION=$(perl -e '"'"$PKG_NAME_AND_VERSION"'" =~ m/(\d+(\.\d+)+)/; print "$1\n";')
echo "According to $RPMDEB package, the ceph version under test is ->$PKG_CEPH_VERSION<-"
test -n "$PKG_CEPH_VERSION"
ceph --version
BUFFER=$(ceph --version)
CEPH_CEPH_VERSION=$(perl -e '"'"$BUFFER"'" =~ m/ceph version (\d+(\.\d+)+)/; print "$1\n";')
echo "According to \"ceph --version\", the ceph version under test is ->$CEPH_CEPH_VERSION<-"
test -n "$CEPH_CEPH_VERSION"
test "$PKG_CEPH_VERSION" = "$CEPH_CEPH_VERSION"
