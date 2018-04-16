#!/bin/bash

CURR_DIR=`pwd`
cd ../../../../build
API_URL=`./bin/ceph mgr services 2>/dev/null | jq .dashboard | sed -e 's/"//g' -e 's!/$!!g'`
cd $CURR_DIR

curl -s -c /tmp/cd-cookie.txt -H "Content-Type: application/json" -X POST -d '{"username":"admin","password":"admin"}'  $API_URL/api/auth > /dev/null

echo "API_ENDPOINT: $API_URL"
echo "METHOD: $1"
echo "PATH: $2"
echo "DATA: $3"
echo ""

curl -s -b /tmp/cd-cookie.txt -H "Content-Type: application/json" -X $1 -d "$3" ${API_URL}$2 | jq
