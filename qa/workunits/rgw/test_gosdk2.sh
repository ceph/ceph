#!/bin/sh
#
# To run this test script with a cluster created via vstart.sh:
# $PATH needs to be set for radosgw-admin executables.
# $CEPH_ROOT needs to be set to the path of the Ceph source code
# $RGW_HTTP_ENDPOINT_URL needs to be set to the endpoint of the RGW
#
# Example when ceph source is cloned into $HOME and a vstart cluster is already running with a radosgw:
# $ PATH=~/ceph/build/bin/:$PATH CEPH_ROOT=~/ceph RGW_HTTP_ENDPOINT=http://localhost:8000 ~/ceph/qa/workunits/rgw/test_gosdk2.sh
#

set -x

if [ -z ${AWS_ACCESS_KEY_ID} ]
then
    export AWS_ACCESS_KEY_ID="lNCnR47C2g+ZidCWBAUuwfSAA7Q="
    export AWS_SECRET_ACCESS_KEY="tYuA2Y+Uu1ow2l9Xe59tWKVml3gMuVfyhUjjJwfwEI0vFFONIcqf4g=="

    radosgw-admin user create --uid ceph-test-gosdk2 \
       --access-key $AWS_ACCESS_KEY_ID \
       --secret $AWS_SECRET_ACCESS_KEY \
       --display-name "gosdk2 test user" \
       --email gosdk2@example.com || echo "gosdk2 user exists"
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
fi

# https://stackoverflow.com/questions/7978517/how-do-i-get-cmake-to-work-with-the-go-programming-language

# operations using addiitonal checksums
cd $CEPH_ROOT/qa/workunits/rgw/gcksum

go mod download github.com/aws/aws-sdk-go-v2
go mod download github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream
go mod download github.com/aws/aws-sdk-go-v2/config
go mod download github.com/aws/aws-sdk-go-v2/credentials
go mod download github.com/aws/aws-sdk-go-v2/feature/ec2/imds
go mod download github.com/aws/aws-sdk-go-v2/internal/configsources
go mod download github.com/aws/aws-sdk-go-v2/internal/endpoints/v2
go mod download github.com/aws/aws-sdk-go-v2/internal/ini
go mod download github.com/aws/aws-sdk-go-v2/internal/v4a
go mod download github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding
go mod download github.com/aws/aws-sdk-go-v2/service/internal/checksum
go mod download github.com/aws/aws-sdk-go-v2/service/internal/presigned-url
go mod download github.com/aws/aws-sdk-go-v2/service/internal/s3shared
go mod download github.com/aws/aws-sdk-go-v2/service/s3
go mod download github.com/aws/aws-sdk-go-v2/service/sso
go mod download github.com/aws/aws-sdk-go-v2/service/ssooidc
go mod download github.com/aws/aws-sdk-go-v2/service/sts
go mod download github.com/aws/smithy-go

# depending on the selection of RGW_HTTP_ENDPOINT_URL in the workunit,
# this will test an HTTP or HTTP/s, which is relevant to cover all
# checksum strategies (e.g., header vs trailer, aws-chunked)

go test

exit 0
