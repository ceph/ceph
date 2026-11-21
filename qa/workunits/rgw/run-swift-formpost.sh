#!/usr/bin/env bash
set -eu

mydir=$(dirname "$0")
RGW_HTTP_ENDPOINT_URL=${RGW_HTTP_ENDPOINT_URL:-$(cat "$TESTDIR/url_file")}
export RGW_HTTP_ENDPOINT_URL

python3 -m venv "$mydir"
"$mydir/bin/pip" install boto3 requests
"$mydir/bin/python3" "$mydir/test_swift_formpost_principal_type.py"
