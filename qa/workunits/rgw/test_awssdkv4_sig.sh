#!/bin/sh
#
# To run this test script with a cluster created via vstart.sh:
# $PATH needs to be set for radosgw-admin executables.
# $CEPH_ROOT needs to be set to the path of the Ceph source code
# $RGW_HTTP_ENDPOINT_URL needs to be set to the endpoint of the RGW
#
# Example when ceph source is cloned into $HOME and a vstart cluster is already running with a radosgw:
# $ PATH=~/ceph/build/bin/:$PATH CEPH_ROOT=~/ceph RGW_HTTP_ENDPOINT=http://localhost:8000 ~/ceph/qa/workunits/rgw/test_awssdkv4_sig.sh
#

set -x

if [ -z ${AWS_ACCESS_KEY_ID} ]
then
    export AWS_ACCESS_KEY_ID="lNCnR47C2g+ZidCWBAUuwfSAA7Q="
    export AWS_SECRET_ACCESS_KEY="tYuA2Y+Uu1ow2l9Xe59tWKVml3gMuVfyhUjjJwfwEI0vFFONIcqf4g=="

    radosgw-admin user create --uid ceph-test-maven \
       --access-key $AWS_ACCESS_KEY_ID \
       --secret $AWS_SECRET_ACCESS_KEY \
       --display-name "maven test user" \
       --email sigv4@example.com || echo "sigv4 maven user exists"
fi

if [ -z ${RGW_HTTP_ENDPOINT_URL} ]
then
  # TESTDIR and this block are meant for when this script is run in a teuthology environment
  if [ -z ${TESTDIR} ]
  then
    echo "TESTDIR is not defined, cannot set RGW_HTTP_ENDPOINT_URL in teuthology"
    exit
  else
    export RGW_HTTP_ENDPOINT_URL=$(cat ${TESTDIR}/url_file)
  fi
fi

if [ -z ${CEPH_ROOT} ]
then
  echo "CEPH_ROOT is not defined"
  exit
else
  cd $CEPH_ROOT/qa/workunits/rgw/jcksum
fi

./mvnw clean package
./mvnw test -Dtest=PutObjects

exit
