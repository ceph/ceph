#!/bin/sh -e
#
# To run this test script with a cluster created via vstart.sh:
# $PATH needs to be set for radosgw-admin and ceph_test_librgw executables.
# $KEYRING need to be set as the path for a vstart clusters Ceph keyring.
#
# Example when ceph source is cloned into $HOME and a vstart cluster is already running with a radosgw:
# $ PATH=~/ceph/build/bin/:$PATH KEYRING=~/ceph/build/keyring ~/ceph/qa/workunits/rgw/test_awssdkv4_sig.sh

if [ -z ${AWS_ACCESS_KEY_ID} ]
then
    export AWS_ACCESS_KEY_ID=`openssl rand -base64 20`
    export AWS_SECRET_ACCESS_KEY=`openssl rand -base64 40`

    radosgw-admin user create --uid ceph-test-maven \
       --access-key $AWS_ACCESS_KEY_ID \
       --secret $AWS_SECRET_ACCESS_KEY \
       --display-name "maven test user" \
       --email sigv4@example.com || echo "sigv4 maven user exists"

    # keyring override for teuthology env
    if [ -z ${KEYRING} ]
    then
      KEYRING="/etc/ceph/ceph.keyring"
    fi
    K="-k ${KEYRING}"
fi

# the required S3 access_key and secret_key are already exported above, but
# we need to hook up the S3 endpoints

# the following are taken from

# XXXX ok, so I think there should be only RGW_HTTP_ENDPOINT_URL and that
# it may already be set before this script runs, though if we can figure it
# out, it's ok if we set it...

export RGW_HTTP_ENDPOINT_URL="http://localhost:80"
#export RGW_HTTPS_ENDPOINT_URL="https://localhost:443"

# rgw/test_awssdkv4_sig.sh
pushd jcksum

./mvnw clean package
./mvnw test -Dtest=PutObjects

exit
