#!/bin/sh -ex

p=`uuidgen`

# objects
ceph osd pool create $p 12
ceph osd pool set-quota $p max_objects 10

for f in `seq 1 10` ; do
 rados -p $p put obj$f /etc/passwd
done

sleep 30

rados -p $p put onemore /etc/passwd  && exit 1 || true

ceph osd pool set-quota $p max_objects 100
sleep 30

rados -p $p put onemore /etc/passwd

# bytes
ceph osd pool set-quota $p max_bytes 100
sleep 30

rados -p $p put two /etc/passwd && exit 1 || true

ceph osd pool set-quota $p max_bytes 0
ceph osd pool set-quota $p max_objects 0
sleep 30

rados -p $p put three /etc/passwd

# done
ceph osd pool delete $p $p --yes-i-really-really-mean-it

echo OK

