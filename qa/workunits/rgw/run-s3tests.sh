#!/usr/bin/env bash
set -ex

# run s3-tests from current directory. assume working
# ceph environment (radosgw-admin in path) and rgw on localhost:8000
# (the vstart default).

branch=$1
[ -z "$1" ] && branch=master
port=$2
[ -z "$2" ] && port=8000   # this is vstart's default

##

if [ -e CMakeCache.txt ]; then
    BIN_PATH=$PWD/bin
elif [ -e $root_path/../build/CMakeCache.txt ]; then
    cd $root_path/../build
    BIN_PATH=$PWD/bin
fi
PATH=$PATH:$BIN_PATH

dir=tmp.s3-tests.$$

# clone and bootstrap
mkdir $dir
cd $dir
git clone https://github.com/ceph/s3-tests
cd s3-tests
git checkout ceph-$branch
VIRTUALENV_PYTHON=/usr/bin/python2 ./bootstrap
cd ../..

# users
akey1=access1
skey1=secret1
radosgw-admin user create --uid=s3test1 --display-name='tester1' \
	      --access-key=$akey1 --secret=$skey1 --email=tester1@ceph.com

akey2=access2
skey2=secret2
radosgw-admin user create --uid=s3test2 --display-name='tester2' \
        --access-key=$akey2 --secret=$skey2 --email=tester2@ceph.com

cat <<EOF > s3.conf
[DEFAULT]
## replace with e.g. "localhost" to run against local software
host = 127.0.0.1
## uncomment the port to use something other than 80
port = $port
## say "no" to disable TLS
is_secure = no
[fixtures]
## all the buckets created will start with this prefix;
## {random} will be filled with random characters to pad
## the prefix to 30 characters long, and avoid collisions
bucket prefix = s3testbucket-{random}-
[s3 main]
## the tests assume two accounts are defined, "main" and "alt".
## user_id is a 64-character hexstring
user_id = s3test1
## display name typically looks more like a unix login, "jdoe" etc
display_name = tester1
## replace these with your access keys
access_key = $akey1
secret_key = $skey1
email = tester1@ceph.com
[s3 alt]
## another user account, used for ACL-related tests
user_id = s3test2
display_name = tester2
## the "alt" user needs to have email set, too
email = tester2@ceph.com
access_key = $akey2
secret_key = $skey2
EOF

S3TEST_CONF=`pwd`/s3.conf $dir/s3-tests/virtualenv/bin/nosetests -a '!fails_on_rgw' -v 

rm -rf $dir

echo OK.

