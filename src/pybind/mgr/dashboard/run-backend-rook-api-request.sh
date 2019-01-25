#!/bin/bash

#
# Query k8s to determine where the mgr is running and how to reach the
# dashboard from the local machine. This assumes that the dashboard is being
# exposed via a nodePort service
CURR_DIR=`pwd`
K8S_NAMESPACE='rook-ceph'

HOST=$(kubectl get pods -n $K8S_NAMESPACE -l "app=rook-ceph-mgr" -o json | jq .items[0].spec.nodeName | sed s/\"//g)
PORT=$(kubectl get service -n $K8S_NAMESPACE rook-ceph-mgr-dashboard -o yaml | grep nodePort: | awk '{print $2}')
API_URL="https://${HOST}:${PORT}"

#
# Rook automagically sets up an "admin" account with a random PW and stuffs
# that into a k8s secret. This fetches it.
#
PASSWD=$(kubectl -n $K8S_NAMESPACE get secret rook-ceph-dashboard-password -o yaml | grep "password:" | awk '{print $2}' | base64 --decode)

if [ "$API_URL" = "null" ]; then
	echo "Couldn't retrieve API URL, exiting..." >&2
	exit 1
fi
cd $CURR_DIR

TOKEN=`curl --insecure -s -H "Content-Type: application/json" -X POST \
            -d "{\"username\":\"admin\",\"password\":\"${PASSWD}\"}"  $API_URL/api/auth \
			| jq .token | sed -e 's/"//g'`

echo "METHOD: $1"
echo "URL: ${API_URL}${2}"
echo "DATA: $3"
echo ""

curl --insecure -s -b /tmp/cd-cookie.txt -H "Authorization: Bearer $TOKEN " \
	 -H "Content-Type: application/json" -X $1 -d "$3" ${API_URL}$2 | jq

