#!/usr/bin/env bash
set -ex
mydir=`dirname $0`

python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install redis
pip install configobj
pip install boto3

# create user
radosgw-admin user create --uid=test3 --display-name=test3 --access-key=test3 --secret-key=test3 2>/dev/null

# run test
$mydir/bin/python3 $mydir/test_rgw_d4n.py

deactivate
echo OK.
