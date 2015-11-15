#!/bin/bash -ex

pool=rbd
gen=$pool/gen
data=testfile
size=64
dev=/dev/nbd0

mkdir -p rbd_nbd_test
pushd rbd_nbd_test

function expect_false()
{
  if "$@"; then return 1; else return 0; fi
}

rbd remove $gen || true
rbd-nbd unmap $dev || true

#read test
dd if=/dev/urandom of=$data bs=1M count=$size
rbd --no-progress import $data $gen
rbd-nbd --device $dev map $gen
[ "`dd if=$data bs=1M | md5sum`" != "`dd if=$dev bs=1M | md5sum`" ] && false

#write test
dd if=/dev/urandom of=$data bs=1M count=$size
dd if=$data of=$dev bs=1M
sync
[ "`dd if=$data bs=1M | md5sum`" != "`rbd --no-progress export $gen - | md5sum`" ] && false

#trim test
mkfs.ext4 $dev # better idea?
sync
info=`rbd du $gen | tail -n 1`
[ "`echo $info | awk '{print $2}'`" == "`echo $info | awk '{print $3}'`" ] && false

rbd-nbd unmap $dev
popd
rm -rf rbd_nbd_test

echo OK
