#!/bin/sh -ex

mydir=`dirname $0`

TMP_FILE="/tmp/mon_metadata.json"

function wait_for() {
    local sec=$1
    local cmd=$2

    while true ; do
	$cmd
        if bash -c "$cmd" ; then
            break
        fi
        sec=$(( $sec - 1 ))
        if [ $sec -eq 0 ]; then
            echo failed
            return 1
        fi
        sleep 1
    done
    return 0
}

if ! ceph mgr module ls|jq '.enabled_modules'|grep -q '"prometheus"'; then
    echo "enabling prometheus module"
    ceph mgr module enable prometheus
    wait_for 60 "ceph mgr module ls|jq '.enabled_modules'|grep -q '\"prometheus\"'"
    sleep 10
fi
url=$(ceph mgr dump|jq -r .services.prometheus|sed -e 's/\/$//')
test $url != null
ceph mon metadata --format=json-pretty > $TMP_FILE
echo "url $url $TMP_FILE"
$mydir/test_mgr_prometheus_metrics.py $url $TMP_FILE
rm -f $TMP_FILE

echo $0 OK
