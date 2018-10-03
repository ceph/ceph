#!/usr/bin/env bash
keyfile=`mktemp`
LDAP_USER_KEY_VALUE=${LDAP_USER_KEY_VALUE:-t0pSecret}
sudo RGW_ACCESS_KEY_ID=testuser RGW_SECRET_ACCESS_KEY=${LDAP_USER_KEY_VALUE} radosgw-token --encode --ttype=ldap > ${keyfile}
curdir=`pwd`
retstr=`python ${curdir}/ldap_client.py ${keyfile}`
rm -rf $keyfile
if [[ $retstr == testuser* ]]; then
    echo "LDAP authentication worked"
    exit 0
fi
exit 1
