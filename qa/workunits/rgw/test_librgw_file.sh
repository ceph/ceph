#!/bin/sh -e
#
# To run this test script with a cluster created via vstart.sh:
# $PATH needs to be set for radosgw-admin and ceph_test_librgw executables.
# $KEYRING need to be set as the path for a vstart clusters Ceph keyring.
#
# Example when ceph source is cloned into $HOME and a vstart cluster is already running with a radosgw:
# $ PATH=~/ceph/build/bin/:$PATH KEYRING=~/ceph/build/keyring ~/ceph/qa/workunits/rgw/test_librgw_file.sh

if [ -z ${AWS_ACCESS_KEY_ID} ]
then
    export AWS_ACCESS_KEY_ID=`openssl rand -base64 20`
    export AWS_SECRET_ACCESS_KEY=`openssl rand -base64 40`

    radosgw-admin user create --uid ceph-test-librgw-file \
       --access-key $AWS_ACCESS_KEY_ID \
       --secret $AWS_SECRET_ACCESS_KEY \
       --display-name "librgw test user" \
       --email librgw@example.com || echo "librgw user exists"

    # keyring override for teuthology env
    if [ -z ${KEYRING} ]
    then
      KEYRING="/etc/ceph/ceph.keyring"
    fi
    K="-k ${KEYRING}"
fi

# nfsns is the main suite

# create hierarchy, and then list it
echo "phase 1.1"
ceph_test_librgw_file_nfsns ${K} --hier1 --dirs1 --create --rename --verbose

# the older librgw_file can consume the namespace
echo "phase 1.2"
ceph_test_librgw_file_nfsns ${K} --getattr --verbose

# and delete the hierarchy
echo "phase 1.3"
ceph_test_librgw_file_nfsns ${K} --hier1 --dirs1 --delete --verbose

# bulk create/delete buckets
echo "phase 2.1"
ceph_test_librgw_file_cd ${K} --create --multi --verbose
echo "phase 2.2"
ceph_test_librgw_file_cd ${K} --delete --multi --verbose

# write continuation test
echo "phase 3.1"
ceph_test_librgw_file_aw ${K} --create --large --verify
echo "phase 3.2"
ceph_test_librgw_file_aw ${K} --delete --large

# continued readdir
echo "phase 4.1"
ceph_test_librgw_file_marker ${K} --create --marker1 --marker2 --nobjs=100 --verbose
echo "phase 4.2"
ceph_test_librgw_file_marker ${K} --delete --verbose

# advanced i/o--but skip readv/writev for now--split delete from
# create and stat ops to avoid fault in sysobject cache
echo "phase 5.1"
ceph_test_librgw_file_gp ${K} --get --stat --put --create
echo "phase 5.2"
ceph_test_librgw_file_gp ${K} --delete

# rename tests
echo "phase 6.1"
ceph_test_librgw_file_rename ${K} --create

exit 0
