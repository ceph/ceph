#!/usr/bin/env bash

# Wrapper for test_rgw_slo_manifest.py.
#
# Sets up a python virtual environment with the 'requests' dependency,
# creates the Swift TempAuth subuser the test expects, points the test at
# the local RGW, and runs it.
#
# In teuthology the rgw task listens on port 80; for a local vstart run
# export RGW_PORT=8000 (vstart's default) before invoking this script.

set -e

mydir=$(dirname $0)

rgw_host="$(hostname --fqdn)"
echo "INFO: fully qualified domain name: $rgw_host"

export RGW_HOST="${RGW_HOST:-$rgw_host}"
export RGW_PORT="${RGW_PORT:-80}"


# CREATE SWIFT TEMPAUTH USER IF NECESSARY

# the test authenticates as test:tester / testing via /auth/1.0
if radosgw-admin user info --subuser=test:tester 2>/dev/null | grep -q '"user": "test:tester"' ;then
    echo INFO: swift subuser already exists
else
    echo INFO: creating swift subuser test:tester
    radosgw-admin user create --subuser=test:tester \
		  --display-name=Tester-Subuser --key-type=swift \
		  --secret=testing --access=full 2>/dev/null
fi


# SET UP PYTHON VIRTUAL ENVIRONMENT

# install requests
python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install requests


# RUN SLO MANIFEST TEST

# do not abort on a test failure; capture the exit code so we can clean up
set +e
$mydir/bin/python3 ${mydir}/test_rgw_slo_manifest.py
result=$?
set -e


# CLEAN UP

deactivate


# PRODUCE FINAL RESULTS

if [ $result -ne 0 ] ;then
    echo "== FAILED =="
    exit 1
fi

echo "== PASSED =="
exit 0
