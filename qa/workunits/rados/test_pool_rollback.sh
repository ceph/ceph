#!/bin/sh -ex

pool=`uuidgen`

# create pool
ceph osd pool create $pool 16

#mksnap snap1
rados -p $pool mksnap snap1

#create objects
rados -p $pool put obj1 /etc/passwd
rados -p $pool put obj2 /etc/passwd
rados -p $pool put obj3 /etc/passwd

rados -p $pool ls

#mksnap snap2
rados -p $pool mksnap snap2

#create objects
rados -p $pool put obj4 /etc/passwd
rados -p $pool put obj5 /etc/passwd

rados -p $pool ls

#rollback entire pool to snap2
rados -p $pool pool-rollback snap2

rados -p $pool ls

#rollback entire pool to snap1
rados -p $pool pool-rollback snap1

rados -p $pool ls

ceph osd pool delete $pool $pool --yes-i-really-really-mean-it

echo OK

