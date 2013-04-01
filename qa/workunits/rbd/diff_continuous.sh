#!/bin/bash -ex

max=20
size=1500

iosize=16384
iototal=16384000
iothreads=16

parent=`uuidgen`"-parent"
src=`uuidgen`"-src";
dst=`uuidgen`"-dst";

function cleanup() {
    rbd snap purge $src || :
    rbd rm $src || :
    rbd snap purge $dst || :
    rbd rm $dst || :
    rbd snap unprotect $parent --snap parent || :
    rbd snap purge $parent || :
    rbd rm $parent || :
}
trap cleanup EXIT

# start from a clone
rbd create $parent --size $size --image-format 2 --stripe-count 8 --stripe-unit 65536
rbd bench-write $parent --io-size $iosize --io-threads $iothreads --io-total $iototal --io-pattern rand 
rbd snap create $parent --snap parent
rbd snap protect $parent --snap parent
rbd clone $parent@parent $src --stripe-count 4 --stripe-unit 262144
rbd create $dst --size $size --image-format 2 --order 19

# mirror for a while
for s in `seq 1 $max`; do
    rbd snap create $src --snap=snap$s
    rbd export-diff $src@snap$s - $lastsnap | rbd import-diff - $dst  &
    rbd bench-write $src --io-size $iosize --io-threads $iothreads --io-total $iototal --io-pattern rand  &
    wait
    lastsnap="--from-snap snap$s"
done

#trap "" EXIT
#exit 0

# validate
for s in `seq 1 $max`; do
    ssum=`rbd export $src@snap$s - | md5sum`
    dsum=`rbd export $dst@snap$s - | md5sum`
    if [ "$ssum" != "$dsum" ]; then
	echo different sum at snap$s
	exit 1
    fi
done

cleanup
trap "" EXIT

echo OK

