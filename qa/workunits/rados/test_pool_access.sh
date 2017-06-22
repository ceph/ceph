#!/bin/bash -x

set -e

expect_1()
{
  set -x
  set +e
  "$@"
  if [ $? == 1 ]; then return 0; else return 1; fi
}


key=`ceph auth get-or-create-key client.poolaccess1 mon 'allow r' osd 'allow *'`
rados --id poolaccess1 --key $key -p rbd ls

key=`ceph auth get-or-create-key client.poolaccess2 mon 'allow r' osd 'allow * pool=nopool'`
expect_1 rados --id poolaccess2 --key $key -p rbd ls

key=`ceph auth get-or-create-key client.poolaccess3 mon 'allow r' osd 'allow rw pool=nopool'`
expect_1 rados --id poolaccess3 --key $key -p rbd ls

echo OK
