#!/bin/sh -ex

mydir=`dirname $0`

secret=`ceph config-key get mgr/restful/keys/admin`
url=$(ceph mgr dump|jq -r .services.restful|sed -e 's/\/$//')
echo "url $url secret $secret"
$mydir/test_mgr_rest_api.py $url $secret

echo $0 OK
