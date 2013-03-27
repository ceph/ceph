#!/bin/bash -ex

max=30
size=1500

iosize=16384
iototal=16384000
iothreads=16

src=`uuidgen`"-src";
dst=`uuidgen`"-dst";

function cleanup() {
    rbd snap purge $src || :
    rbd rm $src || :
    rbd snap purge $dst || :
    rbd rm $dst || :
}
trap cleanup EXIT

rbd create $src --size $size
rbd create $dst --size $size

# mirror for a while

rbd snap create $src --snap=snap0
rbd bench-write $src --io-size $iosize --io-threads $iothreads --io-total $iototal --io-pattern rand 
lastsnap=snap0
for s in `seq 1 $max`; do
    rbd snap create $src --snap=snap$s
    rbd export-diff $src@snap$s - --from-snap $lastsnap | rbd import-diff - $dst  &
    rbd bench-write $src --io-size $iosize --io-threads $iothreads --io-total $iototal --io-pattern rand  &
    wait
    lastsnap=snap$s
done

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

