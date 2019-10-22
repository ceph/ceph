# ceph_version_sanity.sh
#
# test that ceph RPM version matches "ceph --version"
# for a loose definition of "matches"
#
# args: None

set -ex
rpm -q ceph
RPM_NAME=$(rpm -q ceph)
RPM_CEPH_VERSION=$(perl -e '"'"$RPM_NAME"'" =~ m/ceph-(\d+\.\d+\.\d+)/; print "$1\n";')
echo "According to RPM, the ceph upstream version is ->$RPM_CEPH_VERSION<-" >/dev/null
test -n "$RPM_CEPH_VERSION"
ceph --version
BUFFER=$(ceph --version)
CEPH_CEPH_VERSION=$(perl -e '"'"$BUFFER"'" =~ m/ceph version (\d+\.\d+\.\d+)/; print "$1\n";')
echo "According to \"ceph --version\", the ceph upstream version is ->$CEPH_CEPH_VERSION<-" \
    >/dev/null
test -n "$RPM_CEPH_VERSION"
test "$RPM_CEPH_VERSION" = "$CEPH_CEPH_VERSION"
echo "OK" >/dev/null
