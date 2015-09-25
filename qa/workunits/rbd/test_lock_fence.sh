#!/bin/bash  -x
# can't use -e because of background process

IMAGE=rbdrw-image
LOCKID=rbdrw
RBDRW=rbdrw.py
CEPH_REF=${CEPH_REF:-master}

wget -O $RBDRW "https://git.ceph.com/?p=ceph.git;a=blob_plain;hb=$CEPH_REF;f=src/test/librbd/rbdrw.py"

rbd create $IMAGE --size 10 --image-format 2 --image-shared || exit 1

# rbdrw loops doing I/O to $IMAGE after locking with lockid $LOCKID
python $RBDRW $IMAGE $LOCKID &
iochild=$!

# give client time to lock and start reading/writing
LOCKS='{}'
while [ "$LOCKS" == "{}" ]
do
    LOCKS=$(rbd lock list $IMAGE --format json)
    sleep 1
done

clientaddr=$(rbd lock list $IMAGE | tail -1 | awk '{print $NF;}')
clientid=$(rbd lock list $IMAGE | tail -1 | awk '{print $1;}')
echo "clientaddr: $clientaddr"
echo "clientid: $clientid"

ceph osd blacklist add $clientaddr || exit 1

wait $iochild
rbdrw_exitcode=$?
if [ $rbdrw_exitcode != 108 ]
then
	echo "wrong exitcode from rbdrw: $rbdrw_exitcode"
	exit 1
else
	echo "rbdrw stopped with ESHUTDOWN"
fi

set -e
ceph osd blacklist rm $clientaddr
rbd lock remove $IMAGE $LOCKID "$clientid"
# rbdrw will have exited with an existing watch, so, until #3527 is fixed,
# hang out until the watch expires
sleep 30
rbd rm $IMAGE
echo OK
