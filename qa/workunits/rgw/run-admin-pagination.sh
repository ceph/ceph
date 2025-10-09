#!/usr/bin/env bash
set -ex

mydir=`dirname $0`

python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install boto3 requests

## run test
$mydir/bin/python3 $mydir/test_rgw_admin_pagination.py

deactivate
echo OK.
