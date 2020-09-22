#!/bin/sh -e

export AWS_ACCESS_KEY_ID=`openssl rand -base64 20`
export AWS_SECRET_ACCESS_KEY=`openssl rand -base64 40`

radosgw-admin user create --uid ceph-test-librgw-file \
       --access-key $AWS_ACCESS_KEY_ID \
       --secret $AWS_SECRET_ACCESS_KEY \
       --display-name "librgw test user" \
       --email librgw@example.com || echo "librgw user exists"

ceph_test_librgw_file
ceph_test_librgw_file_aw
ceph_test_librgw_file_cd
ceph_test_librgw_file_gp
ceph_test_librgw_file_marker
ceph_test_librgw_file_nfsns

exit 0
