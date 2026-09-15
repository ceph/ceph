#!/usr/bin/env bash
set -ex

# assume working ceph environment (radosgw-admin in path) and rgw on localhost:80

mydir=`dirname $0`

python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install boto3

$mydir/bin/python3 $mydir/test_rgw_multipart_category.py

deactivate
echo OK.
