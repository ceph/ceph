#!/bin/sh -ex

p=`uuidgen`

# objects
ceph osd pool create $p 12
ceph osd pool set-quota $p max_objects 10

for f in `seq 1 10` ; do
 rados -p $p put obj$f /etc/passwd
done

sleep 30

rados -p $p put onemore /etc/passwd  &
pid=$!

ceph osd pool set-quota $p max_objects 100
wait $pid 
[ $? -ne 0 ] && exit 1 || true

rados -p $p put twomore /etc/passwd

# bytes
ceph osd pool set-quota $p max_bytes 100
sleep 30

rados -p $p put two /etc/passwd &
pid=$!

ceph osd pool set-quota $p max_bytes 0
ceph osd pool set-quota $p max_objects 0
wait $pid 
[ $? -ne 0 ] && exit 1 || true

rados -p $p put three /etc/passwd


#one pool being full does not block a different pool

pp=`uuidgen`

ceph osd pool create $pp 12

# set objects quota 
ceph osd pool set-quota $pp max_objects 10
sleep 30

for f in `seq 1 10` ; do
 rados -p $pp put obj$f /etc/passwd
done

sleep 30

rados -p $p put threemore /etc/passwd 

ceph osd pool set-quota $p max_bytes 0
ceph osd pool set-quota $p max_objects 0

sleep 30
# done
ceph osd pool delete $p $p --yes-i-really-really-mean-it
ceph osd pool delete $pp $pp --yes-i-really-really-mean-it

echo OK

