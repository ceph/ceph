#!/usr/bin/env bash

set -x

die() {
    echo "$@"
    exit 1
}

do_run() {
    if [ "$1" == "--tee" ]; then
      shift
      tee_out="$1"
      shift
      "$@" | tee $tee_out
    else
      "$@"
    fi
}

run_expect_succ() {
    echo "RUN_EXPECT_SUCC: " "$@"
    do_run "$@"
    [ $? -ne 0 ] && die "expected success, but got failure! cmd: $@"
}

run() {
    echo "RUN: " $@
    do_run "$@"
}

if [ -n "$CEPH_BIN" ] ; then
   # CMake env
   RADOS_TOOL="$CEPH_BIN/rados"
   CEPH_TOOL="$CEPH_BIN/ceph"
   DEDUP_TOOL="$CEPH_BIN/ceph-dedup-tool"
else
   # executables should be installed by the QA env 
   RADOS_TOOL=$(which rados)
   CEPH_TOOL=$(which ceph)
   DEDUP_TOOL=$(which ceph-dedup-tool)
fi

POOL=dedup_pool
OBJ=test_rados_obj

[ -x "$RADOS_TOOL" ] || die "couldn't find $RADOS_TOOL binary to test"
[ -x "$CEPH_TOOL" ] || die "couldn't find $CEPH_TOOL binary to test"

run_expect_succ "$CEPH_TOOL" osd pool create "$POOL" 8

function test_dedup_ratio_fixed()
{
  # case 1
  dd if=/dev/urandom of=dedup_object_1k bs=1K count=1
  dd if=dedup_object_1k of=dedup_object_100k bs=1K count=100

  $RADOS_TOOL -p $POOL put $OBJ ./dedup_object_100k
  RESULT=$($DEDUP_TOOL --op estimate --pool $POOL --chunk-size 1024  --chunk-algorithm fixed --fingerprint-algorithm sha1 --debug | grep result | awk '{print$4}')
  if [ 1024 -ne $RESULT ];
  then
    die "Estimate failed expecting 1024 result $RESULT"
  fi

  # case 2
  dd if=/dev/zero of=dedup_object_10m bs=10M count=1

  $RADOS_TOOL -p $POOL put $OBJ ./dedup_object_10m
  RESULT=$($DEDUP_TOOL --op estimate --pool $POOL --chunk-size 4096  --chunk-algorithm fixed --fingerprint-algorithm sha1 --debug | grep result | awk '{print$4}')
  if [ 4096 -ne $RESULT ];
  then
    die "Estimate failed expecting 4096 result $RESULT"
  fi

  # case 3 max_thread
  for num in `seq 0 20`
  do
    dd if=/dev/zero of=dedup_object_$num bs=4M count=1
    $RADOS_TOOL -p $POOL put dedup_object_$num ./dedup_object_$num
  done

  RESULT=$($DEDUP_TOOL --op estimate --pool $POOL --chunk-size 4096  --chunk-algorithm fixed --fingerprint-algorithm sha1 --max-thread 4 --debug | grep result | awk '{print$2}')

  if [ 98566144 -ne $RESULT ];
  then
    die "Estimate failed expecting 98566144 result $RESULT"
  fi

  rm -rf ./dedup_object_1k ./dedup_object_100k ./dedup_object_10m
  for num in `seq 0 20`
  do
    rm -rf ./dedup_object_$num
  done
  $RADOS_TOOL -p $POOL rm $OBJ 
  for num in `seq 0 20`
  do
    $RADOS_TOOL -p $POOL rm dedup_object_$num
  done
}

test_dedup_ratio_fixed

$CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it

echo "SUCCESS!"
exit 0


