#!/usr/bin/env bash
set -ex
mydir=`dirname $0`

python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install redis
pip install configobj
pip install boto3

#mount -t tmpfs -o size=15 tmpfs /tmp/rgw_d4n_datacache

# run test
$mydir/bin/python3 $mydir/test_rgw_d4n_remote.py

deactivate

sudo radosgw-admin -n client.0 user rm --uid s3main --purge-data
echo OK.
