#!/usr/bin/env bash
set -ex

# this test uses fault injection to abort during 'radosgw-admin bucket reshard'
# disable coredumps so teuthology won't mark a failure
ulimit -c 0

#assume working ceph environment (radosgw-admin in path) and rgw on localhost:80
# localhost::443 for ssl

mydir=`dirname $0`

python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install boto3

## run test
$mydir/bin/python3 $mydir/test_rgw_reshard.py

deactivate
echo OK.

