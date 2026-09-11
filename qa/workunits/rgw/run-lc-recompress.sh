#!/usr/bin/env bash
set -ex

mydir=$(dirname $0)

python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install boto3

$mydir/bin/python3 $mydir/test_rgw_lc_recompress.py

deactivate
echo OK.
