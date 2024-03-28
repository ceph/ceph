#!/bin/bash

CURR_DIR=`pwd`
[ -z "$BUILD_DIR" ] && BUILD_DIR=build
cd ../../../../${BUILD_DIR}
API_URL=`./bin/ceph mgr services 2>/dev/null | jq .dashboard | sed -e 's/"//g' -e 's!/$!!g'`
if [ "$API_URL" = "null" ]; then
	echo "Couldn't retrieve API URL, exiting..." >&2
	exit 1
fi
cd $CURR_DIR

TOKEN=`curl --insecure -s -H "Content-Type: application/json" -X POST \
            -d '{"username":"admin","password":"admin"}'  $API_URL/api/auth \
			| jq .token | sed -e 's/"//g'`

echo "METHOD: $1"
echo "URL: ${API_URL}${2}"
echo "DATA: $3"
echo ""

curl --insecure -s -b /tmp/cd-cookie.txt -H "Authorization: Bearer $TOKEN " \
	 -H "Content-Type: application/json" -X $1 -d "$3" ${API_URL}$2 | jq

