#!/bin/sh -ex

cauthtool --create-keyring k --gen-key -p --name client.xx
ceph auth add -i k client.xx mon 'allow command foo; allow command bar'

( ceph -k k -n client.xx foo || true ) | grep -v 'Access denied'
( ceph -k k -n client.xx foo ooo || true ) | grep -v 'Access denied'
( ceph -k k -n client.xx fo || true ) | grep 'Access denied'
( ceph -k k -n client.xx fooo || true ) | grep 'Access denied'

( ceph -k k -n client.xx bar || true ) | grep -v 'Access denied'
( ceph -k k -n client.xx bar a b c || true ) | grep -v 'Access denied'
( ceph -k k -n client.xx ba || true ) | grep 'Access denied'
( ceph -k k -n client.xx barr || true ) | grep 'Access denied'

( ceph -k k -n client.xx baz || true ) | grep 'Access denied'

echo OK