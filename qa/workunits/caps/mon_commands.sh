#!/bin/sh -ex

ceph-authtool --create-keyring k --gen-key -p --name client.xx
ceph auth add -i k client.xx mon "allow command foo; allow command bar *; allow command baz ...; allow command foo add * mon allow\\ rwx osd allow\\ *"

( ceph -k k -n client.xx foo      || true ) | grep 'unrecog'
( ceph -k k -n client.xx foo ooo  || true ) | grep 'Access denied'
( ceph -k k -n client.xx fo       || true ) | grep 'Access denied'
( ceph -k k -n client.xx fooo     || true ) | grep 'Access denied'

( ceph -k k -n client.xx bar       || true ) | grep 'Access denied'
( ceph -k k -n client.xx bar a     || true ) | grep 'unrecog'
( ceph -k k -n client.xx bar a b c || true ) | grep 'Access denied'
( ceph -k k -n client.xx ba        || true ) | grep 'Access denied'
( ceph -k k -n client.xx barr      || true ) | grep 'Access denied'

( ceph -k k -n client.xx baz     || true ) | grep -v 'Access denied'
( ceph -k k -n client.xx baz a   || true ) | grep -v 'Access denied'
( ceph -k k -n client.xx baz a b || true ) | grep -v 'Access denied'

( ceph -k k -n client.xx foo add osd.1 -i k mon 'allow rwx' osd 'allow *' || true ) | grep 'unrecog'
( ceph -k k -n client.xx foo add osd a b c -i k mon 'allow rwx' osd 'allow *' || true ) | grep 'Access denied'
( ceph -k k -n client.xx foo add osd a b c -i k mon 'allow *' || true ) | grep 'Access denied'

echo OK