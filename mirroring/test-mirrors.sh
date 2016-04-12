#!/bin/bash
#
# Simple script which performs a HTTP and rsync check on
# all Ceph mirrors over IPv4 and IPv6 to see if they are online
#
# Requires IPv4, IPv6, rsync and curl
#
# Example usage:
# - ./test-mirrors.sh eu.ceph.com,de.ceph.com,hk.ceph.com
# - cat MIRRORS |cut -d ':' -f 1|xargs -n 1 ./test-mirrors.sh
#

function print_usage {
    echo "Usage: $0 mirror1,mirror2,mirror3,mirror4,etc"
}

function test_http {
    HOST=$1

    echo -n "$HOST HTTP IPv4: "
    curl -s -I -4 -o /dev/null http://$HOST
    if [ "$?" -ne 0 ]; then
        echo "FAIL"
    else
        echo "OK"
    fi

    echo -n "$HOST HTTP IPv6: "
    curl -s -I -6 -o /dev/null http://$HOST
    if [ "$?" -ne 0 ]; then
        echo "FAIL"
    else
        echo "OK"
    fi
}

function test_rsync {
    HOST=$1

    echo -n "$HOST RSYNC IPv4: "
    rsync -4 -avrqn ${HOST}::ceph /tmp 2>/dev/null
    if [ "$?" -ne 0 ]; then
        echo "FAIL"
    else
        echo "OK"
    fi

    echo -n "$HOST RSYNC IPv6: "
    rsync -6 -avrqn ${HOST}::ceph /tmp 2>/dev/null
    if [ "$?" -ne 0 ]; then
        echo "FAIL"
    else
        echo "OK"
    fi
}

MIRRORS=$1

if [ -z "$MIRRORS" ]; then
    print_usage
    exit 1
fi

IFS=', ' read -r -a array <<< "$MIRRORS"

for MIRROR in "${array[@]}"; do
    test_http $MIRROR
    test_rsync $MIRROR
done
