#!/usr/bin/bash -ex

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}


ceph auth export
ceph auth rm client.rot

ceph auth get-or-create client.rot mon 'allow rwx'
ceph auth export client.rot | grep key
ceph auth export client.rot | expect_false grep pending.key

ceph auth get-or-create-pending client.rot
ceph auth export client.rot | grep key
ceph auth export client.rot | grep pending.key

ceph auth clear-pending client.rot
ceph auth export client.rot | expect_false grep pending.key

ceph auth get-or-create-pending client.rot
ceph auth export client.rot | grep key
ceph auth export client.rot | grep pending.key
K=$(ceph auth export client.rot | grep 'key = ' | head -n 1 | awk '{print $3}')
PK=$(ceph auth export client.rot | grep pending.key | awk '{print $4}')
echo "K is $K"
echo "PK is $PK"
ceph -n client.rot --key $K -s

ceph auth commit-pending client.rot
ceph auth export client.rot | expect_false grep pending.key
ceph auth export client.rot | grep key | grep $PK

ceph auth get-or-create-pending client.rot
ceph auth export client.rot | grep key
ceph auth export client.rot | grep pending.key
K=$(ceph auth export client.rot | grep 'key = ' | head -n 1 | awk '{print $3}')
PK=$(ceph auth export client.rot | grep pending.key | awk '{print $4}')
echo "2, K is $K"
echo "2, PK is $PK"

ceph auth export client.rot

while ceph -n client.rot --key $K -s ; do
    ceph auth export client.rot
    ceph -n client.rot --key $PK -s
    sleep 1
done

ceph auth export client.rot | expect_false grep pending.key
ceph auth export client.rot | grep key | grep $PK

ceph -n client.rot --key $PK -s

echo ok
