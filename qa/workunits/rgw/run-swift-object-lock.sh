#!/usr/bin/env bash
set -ex

# assume working ceph environment (radosgw-admin in path) and rgw on localhost:80
# localhost::443 for ssl, with the swift and swift_auth apis enabled

mydir=`dirname $0`

python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install boto3 python-swiftclient

## run test
$mydir/bin/python3 $mydir/test_rgw_swift_object_lock.py

deactivate
echo OK.
