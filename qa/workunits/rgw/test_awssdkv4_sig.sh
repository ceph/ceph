#!/bin/sh
#
# To run this test script with a cluster created via vstart.sh:
# $PATH needs to be set for radosgw-admin executables.
# $CEPH_ROOT needs to be set to the path of the Ceph source code
# $RGW_HTTP_ENDPOINT_URL needs to be set to the endpoint of the RGW
#
# Example when ceph source is cloned into $HOME and a vstart cluster is already running with a radosgw:
# $ PATH=~/ceph/build/bin/:$PATH CEPH_ROOT=~/ceph RGW_HTTP_ENDPOINT_URL=http://localhost:8000 ~/ceph/qa/workunits/rgw/test_awssdkv4_sig.sh
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

# we were getting 429 HTTP responses from:
# https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.6/apache-maven-3.9.6-bin.zip
# so the code below allows us to point at a different repo

# default_maven_repo="https://repo.maven.apache.org/maven2"

alt_maven_repo="https://maven-central.storage-download.googleapis.com/maven2"

gradle_settings="~/.gradle/init.gradle"
maven_settings="/tmp/test_awssdkv4_sig-settings-${$}.xml"

# insures all subdirectories exist for the file path given as $1
insure_subdir() {
    dir=$(dirname $1)
    if [ -d $dir ] ;then
	:
    else
	mkdir -p $dir
    fi
}

insure_subdir $gradle_settings
cat >${gradle_settings} <<EOF
pluginManagement {
    repositories {
        maven {
            url "$alt_maven_repo"
        }
    }
}
EOF

insure_subdir $maven_settings
cat >$maven_settings <<EOF
<settings>
  <mirrors>
    <mirror>
      <id>alternative-repo</id>
      <name>Alternative Repository Mirror</name>
      <!-- Your local proxy, company Nexus/Artifactory, or cloud mirror URL -->
      <url>${alt_maven_repo}</url>
      <mirrorOf>*</mirrorOf>
    </mirror>
  </mirrors>
</settings>
EOF

export MVNW_REPOURL="$alt_maven_repo"

./mvnw clean package -s $maven_settings
./mvnw test -Dtest=PutObjects -s $maven_settings

rm -f $maven_settings $gradle_savings

exit
