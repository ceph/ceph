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
   DEDUP_DAEMON="$CEPH_BIN/ceph-dedup-daemon"
else
   # executables should be installed by the QA env 
   RADOS_TOOL=$(which rados)
   CEPH_TOOL=$(which ceph)
   DEDUP_TOOL=$(which ceph-dedup-tool)
   DEDUP_DAEMON=$(which ceph-dedup-daemon)
fi

POOL=dedup_pool
OBJ=test_rados_obj

[ -x "$RADOS_TOOL" ] || die "couldn't find $RADOS_TOOL binary to test"
[ -x "$CEPH_TOOL" ] || die "couldn't find $CEPH_TOOL binary to test"

run_expect_succ "$CEPH_TOOL" osd pool create "$POOL" 8
sleep 5

function test_dedup_ratio_fixed()
{
  # case 1
  dd if=/dev/urandom of=dedup_object_1k bs=1K count=1
  for num in `seq 1 50`
  do
    dd if=dedup_object_1k of=dedup_object_100k bs=1K oflag=append conv=notrunc
  done
  for num in `seq 1 50`
  do
    dd if=/dev/zero of=dedup_object_100k bs=1K count=1 oflag=append conv=notrunc
  done

  $RADOS_TOOL -p $POOL put $OBJ ./dedup_object_100k
  RESULT=$($DEDUP_TOOL --op estimate --pool $POOL --chunk-size 1024  --chunk-algorithm fixed --fingerprint-algorithm sha1 | grep chunk_size_average | awk '{print$2}' | sed "s/\,//g")
  # total size / the number of deduped object = 100K / 1
  if [ 51200 -ne $RESULT ];
  then
    die "Estimate failed expecting 51200 result $RESULT"
  fi

  # case 2
  dd if=/dev/zero of=dedup_object_10m bs=10M count=1

  $RADOS_TOOL -p $POOL put $OBJ ./dedup_object_10m
  RESULT=$($DEDUP_TOOL --op estimate --pool $POOL --chunk-size 4096  --chunk-algorithm fixed --fingerprint-algorithm sha1 | grep examined_bytes | awk '{print$2}')
  # 10485760
  if [ 10485760 -ne $RESULT ];
  then
    die "Estimate failed expecting 10485760 result $RESULT"
  fi

  # case 3 max_thread
  for num in `seq 0 20`
  do
    dd if=/dev/zero of=dedup_object_$num bs=4M count=1
    $RADOS_TOOL -p $POOL put dedup_object_$num ./dedup_object_$num
  done

  RESULT=$($DEDUP_TOOL --op estimate --pool $POOL --chunk-size 4096  --chunk-algorithm fixed --fingerprint-algorithm sha1 --max-thread 4 | grep chunk_size_average | awk '{print$2}' | sed "s/\,//g")

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

function test_dedup_chunk_scrub()
{

  CHUNK_POOL=dedup_chunk_pool
  run_expect_succ "$CEPH_TOOL" osd pool create "$CHUNK_POOL" 8

  echo "hi there" > foo

  echo "hi there" > bar

  echo "there" > foo-chunk

  echo "CHUNK" > bar-chunk

  $CEPH_TOOL osd pool set $POOL fingerprint_algorithm sha1 --yes-i-really-mean-it
  $CEPH_TOOL osd pool set $POOL dedup_chunk_algorithm fastcdc --yes-i-really-mean-it
  $CEPH_TOOL osd pool set $POOL dedup_cdc_chunk_size 4096 --yes-i-really-mean-it
  $CEPH_TOOL osd pool set $POOL dedup_tier $CHUNK_POOL --yes-i-really-mean-it

  $RADOS_TOOL -p $POOL put foo ./foo
  $RADOS_TOOL -p $POOL put bar ./bar

  $RADOS_TOOL -p $CHUNK_POOL put bar-chunk ./bar-chunk
  $RADOS_TOOL -p $CHUNK_POOL put foo-chunk ./foo-chunk

  $RADOS_TOOL -p $POOL set-chunk bar 0 8 --target-pool $CHUNK_POOL bar-chunk 0 --with-reference

  echo -n "There hi" > test_obj
  # dirty
  $RADOS_TOOL -p $POOL put foo ./test_obj
  $RADOS_TOOL -p $POOL set-chunk foo 0 8 --target-pool $CHUNK_POOL foo-chunk 0 --with-reference
  # flush
  $RADOS_TOOL -p $POOL tier-flush foo
  sleep 2

  $RADOS_TOOL ls -p $CHUNK_POOL
  CHUNK_OID=$(echo -n "There hi" | sha1sum | awk '{print $1}')

  POOL_ID=$($CEPH_TOOL osd pool ls detail | grep $POOL |  awk '{print$2}')
  $DEDUP_TOOL --op chunk-get-ref --chunk-pool $CHUNK_POOL --object $CHUNK_OID --target-ref bar --target-ref-pool-id $POOL_ID
  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID)

  RESULT=$($DEDUP_TOOL --op chunk-scrub --chunk-pool $CHUNK_POOL | grep "Damaged object" | awk '{print$4}')
  if [ $RESULT -ne "1" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Chunk-scrub failed expecting damaged objects is not 1"
  fi

  $DEDUP_TOOL --op chunk-put-ref --chunk-pool $CHUNK_POOL --object $CHUNK_OID --target-ref bar --target-ref-pool-id $POOL_ID
  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID | grep bar)
  if [ -n "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Scrub failed expecting bar is removed"
  fi

  $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it

  rm -rf ./foo ./bar ./foo-chunk ./bar-chunk ./test_obj
  $RADOS_TOOL -p $POOL rm foo
  $RADOS_TOOL -p $POOL rm bar
}

function test_dedup_chunk_repair()
{

  CHUNK_POOL=dedup_chunk_pool
  run_expect_succ "$CEPH_TOOL" osd pool create "$CHUNK_POOL" 8

  echo -n "hi there" > foo

  echo -n "hi there" > bar

  echo -n "there" > foo-chunk

  echo -n "CHUNK" > bar-chunk

  $CEPH_TOOL osd pool set $POOL fingerprint_algorithm sha1 --yes-i-really-mean-it
  $CEPH_TOOL osd pool set $POOL dedup_chunk_algorithm fastcdc --yes-i-really-mean-it
  $CEPH_TOOL osd pool set $POOL dedup_cdc_chunk_size 4096 --yes-i-really-mean-it
  $CEPH_TOOL osd pool set $POOL dedup_tier $CHUNK_POOL --yes-i-really-mean-it

  $RADOS_TOOL -p $POOL put foo ./foo
  $RADOS_TOOL -p $POOL put bar ./bar

  $RADOS_TOOL -p $CHUNK_POOL put bar-chunk ./bar-chunk
  $RADOS_TOOL -p $CHUNK_POOL put foo-chunk ./foo-chunk

  $RADOS_TOOL ls -p $CHUNK_POOL
  CHUNK_OID=$(echo -n "hi there" | sha1sum | awk '{print $1}')

  POOL_ID=$($CEPH_TOOL osd pool ls detail | grep $POOL |  awk '{print$2}')
  $RADOS_TOOL -p $CHUNK_POOL put $CHUNK_OID ./foo

  # increase ref count by two, resuling in mismatch
  $DEDUP_TOOL --op chunk-get-ref --chunk-pool $CHUNK_POOL --object $CHUNK_OID --target-ref foo --target-ref-pool-id $POOL_ID
  $DEDUP_TOOL --op chunk-get-ref --chunk-pool $CHUNK_POOL --object $CHUNK_OID --target-ref foo --target-ref-pool-id $POOL_ID
  $DEDUP_TOOL --op chunk-get-ref --chunk-pool $CHUNK_POOL --object $CHUNK_OID --target-ref foo --target-ref-pool-id $POOL_ID
  $DEDUP_TOOL --op chunk-get-ref --chunk-pool $CHUNK_POOL --object $CHUNK_OID --target-ref foo --target-ref-pool-id $POOL_ID
  $DEDUP_TOOL --op chunk-get-ref --chunk-pool $CHUNK_POOL --object bar-chunk --target-ref bar --target-ref-pool-id $POOL_ID
  $DEDUP_TOOL --op chunk-get-ref --chunk-pool $CHUNK_POOL --object bar-chunk --target-ref bar --target-ref-pool-id $POOL_ID

  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID)
  RESULT=$($DEDUP_TOOL --op chunk-scrub --chunk-pool $CHUNK_POOL | grep "Damaged object" | awk '{print$4}')
  if [ $RESULT -ne "2" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Chunk-scrub failed expecting damaged objects is not 1"
  fi

  $DEDUP_TOOL --op chunk-repair --chunk-pool $CHUNK_POOL --object $CHUNK_OID --target-ref foo --target-ref-pool-id $POOL_ID
  $DEDUP_TOOL --op chunk-repair --chunk-pool $CHUNK_POOL --object bar-chunk --target-ref bar --target-ref-pool-id $POOL_ID

  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID | grep foo | wc -l)
  if [ 0 -ne "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Scrub failed expecting bar is removed"
  fi
  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object bar-chunk | grep bar | wc -l)
  if [ 0 -ne "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Scrub failed expecting bar is removed"
  fi

  $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it

  rm -rf ./foo ./bar ./foo-chunk ./bar-chunk ./test_obj
  $RADOS_TOOL -p $POOL rm foo
  $RADOS_TOOL -p $POOL rm bar
}

function test_dedup_object()
{

  CHUNK_POOL=dedup_chunk_pool
  run_expect_succ "$CEPH_TOOL" osd pool create "$CHUNK_POOL" 8

  echo "There hiHI" > foo

  $CEPH_TOOL osd pool set $POOL dedup_tier $CHUNK_POOL --yes-i-really-mean-it
  $RADOS_TOOL -p $POOL put foo ./foo

  sleep 2

  rados ls -p $CHUNK_POOL

  RESULT=$($DEDUP_TOOL --pool $POOL --op chunk-dedup --object foo --chunk-pool $CHUNK_POOL --source-off 0 --source-length 10 --fingerprint-algorithm sha1 )

  POOL_ID=$($CEPH_TOOL osd pool ls detail | grep $POOL |  awk '{print$2}')
  CHUNK_OID=$(echo -n "There hiHI" | sha1sum | awk '{print $1}')

  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID | grep foo)

  if [ -z "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Scrub failed expecting bar is removed"
  fi

  $RADOS_TOOL -p $CHUNK_POOL get $CHUNK_OID ./chunk
  VERIFY=$(cat ./chunk | sha1sum | awk '{print $1}')
  if [ "$CHUNK_OID" != "$VERIFY" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Comparing failed expecting chunk mismatch"
  fi

  echo -n "There hihiHI" > bar

  $RADOS_TOOL -p $POOL put bar ./bar
  RESULT=$($DEDUP_TOOL --pool $POOL --op object-dedup --object bar --chunk-pool $CHUNK_POOL --fingerprint-algorithm sha1 --dedup-cdc-chunk-size 4096)

  CHUNK_OID=$(echo -n "There hihiHI" | sha1sum | awk '{print $1}')

  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID | grep bar)
  if [ -z "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Scrub failed expecting bar is removed"
  fi

  $RADOS_TOOL -p $CHUNK_POOL get $CHUNK_OID ./chunk
  VERIFY=$(cat ./chunk | sha1sum | awk '{print $1}')
  if [ "$CHUNK_OID" != "$VERIFY" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Comparing failed expecting chunk mismatch"
  fi

  echo -n "THERE HIHIHI" > bar
  $RADOS_TOOL -p $POOL put bar ./bar
  $RADOS_TOOL -p $POOL mksnap mysnap

  echo -n "There HIHIHI" > bar
  $RADOS_TOOL -p $POOL put bar ./bar

  RESULT=$($DEDUP_TOOL --pool $POOL --op object-dedup --object bar --chunk-pool $CHUNK_POOL --fingerprint-algorithm sha1 --dedup-cdc-chunk-size 4096 --snap)

  CHUNK_OID=$(echo -n "THERE HIHIHI" | sha1sum | awk '{print $1}')
  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID | grep bar)
  if [ -z "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Scrub failed expecting bar is removed"
  fi

  CHUNK_OID=$(echo -n "There HIHIHI" | sha1sum | awk '{print $1}')
  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID | grep bar)
  if [ -z "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Scrub failed expecting bar is removed"
  fi
  # rerun tier-flush

  RESULT=$($DEDUP_TOOL --pool $POOL --op object-dedup --object bar --chunk-pool $CHUNK_POOL --fingerprint-algorithm sha1 --dedup-cdc-chunk-size 4096)

  CHUNK_OID=$(echo -n "There HIHIHI" | sha1sum | awk '{print $1}')
  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID | grep bar)
  if [ -z "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Scrub failed expecting bar is removed"
  fi

  $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it

  rm -rf ./foo ./bar ./chunk
  $RADOS_TOOL -p $POOL rm foo
  $RADOS_TOOL -p $POOL rm bar
}

function test_sample_dedup()
{
  CHUNK_POOL=dedup_chunk_pool
  $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
  $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it

  sleep 2

  run_expect_succ "$CEPH_TOOL" osd pool create "$POOL" 8
  run_expect_succ "$CEPH_TOOL" osd pool create "$CHUNK_POOL" 8
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" dedup_tier "$CHUNK_POOL"
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" dedup_chunk_algorithm fastcdc
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" dedup_cdc_chunk_size 8192
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" fingerprint_algorithm sha1

  # 8 Dedupable objects
  CONTENT_1="There hiHI"
  echo $CONTENT_1 > foo
  for num in `seq 1 8`
  do
    $RADOS_TOOL -p $POOL put foo_$num ./foo
  done

  # 1 Unique object
  CONTENT_3="There hiHI3"
  echo $CONTENT_3 > foo3
  $RADOS_TOOL -p $POOL put foo3_1 ./foo3

  sleep 2

  # Execute dedup crawler
  $DEDUP_DAEMON --pool $POOL --chunk-pool $CHUNK_POOL --chunk-algorithm fastcdc --fingerprint-algorithm sha1 --chunk-dedup-threshold 3 --sampling-ratio 50 --run-once
  sleep 2
  PID=$(pidof ceph-dedup-daemon)
  COUNT=1
  while [ -n "$PID" ] && [ $COUNT -le 30 ]; do
    sleep 15
    PID=$(pidof ceph-dedup-daemon)
    ((COUNT++))
  done

  CHUNK_OID_1=$(echo $CONTENT_1 | sha1sum | awk '{print $1}')
  CHUNK_OID_3=$(echo $CONTENT_3 | sha1sum | awk '{print $1}')

  # Find chunk object has references of 8 dedupable meta objects
  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID_1)
  DEDUP_COUNT=0
  for num in `seq 1 8`
  do
    GREP_RESULT=$(echo $RESULT | grep foo_$num)
    if [ -n "$GREP_RESULT" ]; then
      DEDUP_COUNT=$(($DEDUP_COUNT + 1))
    fi
  done
  if [ $DEDUP_COUNT -lt 2 ]; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Chunk object has no reference of first meta object"
  fi

  sleep 2

  # 7 Duplicated objects but less than chunk dedup threshold
  CONTENT_2="There hiHI2"
  echo $CONTENT_2 > foo2
  for num in `seq 1 7`
  do
    $RADOS_TOOL -p $POOL put foo2_$num ./foo2
  done
  CHUNK_OID_2=$(echo $CONTENT_2 | sha1sum | awk '{print $1}')

  RESULT=$($DEDUP_DAEMON --pool $POOL --chunk-pool $CHUNK_POOL --chunk-algorithm fastcdc --fingerprint-algorithm sha1 --sampling-ratio 100 --chunk-dedup-threshold 2 --max-thread 1 --run-once)
  sleep 2
  PID=$(pidof ceph-dedup-daemon)
  COUNT=1
  while [ -n "$PID" ] && [ $COUNT -le 30 ]; do
    sleep 15
    PID=$(pidof ceph-dedup-daemon)
    ((COUNT++))
  done

  # Objects duplicates less than chunk dedup threshold should be deduplicated because of they satisfies object-dedup-threshold
  # The only object, which is crawled at the very first, should not be deduplicated because it was not duplicated at initial time
  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID_2)
  DEDUP_COUNT=0
  for num in `seq 1 7`
  do
    GREP_RESULT=$(echo $RESULT | grep foo2_$num)
    if [ -n "$GREP_RESULT" ]; then
      DEDUP_COUNT=$(($DEDUP_COUNT + 1))
    fi
  done
  if [ $DEDUP_COUNT -ne 6 ]; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Chunk object has no reference of first meta object"
  fi

  # Unique object should not be deduplicated
  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID_3)
  GREP_RESULT=$($RESULT | grep $CHUNK_OID_3)
  if [ -n "$GREP_RESULT" ]; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "Chunk object has no reference of second meta object"
  fi

  rm -rf ./foo ./foo2 ./foo3
  for num in `seq 1 8`
  do
    $RADOS_TOOL -p $POOL rm foo_$num
  done
  for num in `seq 1 2`
  do
    $RADOS_TOOL -p $POOL rm foo2_$num
  done
  $RADOS_TOOL -p $POOL rm foo3_1

  $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
}

function test_sample_dedup_snap()
{
  CHUNK_POOL=dedup_chunk_pool
  $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
  $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it

  sleep 2

  run_expect_succ "$CEPH_TOOL" osd pool create "$POOL" 8
  run_expect_succ "$CEPH_TOOL" osd pool create "$CHUNK_POOL" 8
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" dedup_tier "$CHUNK_POOL"
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" dedup_chunk_algorithm fastcdc
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" dedup_cdc_chunk_size 8192
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" fingerprint_algorithm sha1

  # 8 Dedupable objects
  CONTENT_1="There hiHI"
  echo $CONTENT_1 > foo
  for num in `seq 1 8`
  do
    $RADOS_TOOL -p $POOL put foo_$num ./foo
  done

  # 1 Unique object
  CONTENT_2="There hiHI3"
  echo $CONTENT_2 > foo3
  $RADOS_TOOL -p $POOL put foo3_1 ./foo3

  $RADOS_TOOL -p $POOL mksnap mysnap

  SNAP_CONTENT="There HIHIHI" 
  echo $SNAP_CONTENT > foo3_new
  $RADOS_TOOL -p $POOL put foo3_1 ./foo3_new

  $RADOS_TOOL -p $POOL mksnap mysnap2
  $RADOS_TOOL -p $POOL put foo3_1 ./foo3_new

  sleep 2

  # Execute dedup crawler
  RESULT=$($DEDUP_DAEMON --pool $POOL --chunk-pool $CHUNK_POOL --chunk-algorithm fastcdc --fingerprint-algorithm sha1 --sampling-ratio 100 --chunk-dedup-threshold 1 --snap --run-once)
  sleep 2
  PID=$(pidof ceph-dedup-daemon)
  COUNT=1
  while [ -n "$PID" ] && [ $COUNT -le 20 ]; do
    sleep 5
    PID=$(pidof ceph-dedup-daemon)
    ((COUNT++))
  done

  CHUNK_OID_2=$(echo $CONTENT_2 | sha1sum | awk '{print $1}')
  SNAP_CONTENT_OID=$(echo $SNAP_CONTENT | sha1sum | awk '{print $1}')

  # Find chunk object has references of 8 dedupable meta objects
  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $SNAP_CONTENT_OID | grep foo3_1)
  if [ -z "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "There is no expected chunk object"
  fi

  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID_2 | grep foo3_1)
  if [ -z "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "There is no expected chunk object"
  fi

  rm -rf ./foo ./foo3 ./foo3_new
  for num in `seq 1 8`
  do
    $RADOS_TOOL -p $POOL rm foo_$num
  done

  $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
}

function test_dedup_memory_limit()
{
  CHUNK_POOL=dedup_chunk_pool
  $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
  $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it

  sleep 2

  run_expect_succ "$CEPH_TOOL" osd pool create "$POOL" 8
  run_expect_succ "$CEPH_TOOL" osd pool create "$CHUNK_POOL" 8

  # 6 dedupable objects
  CONTENT_1="There hiHI"
  echo $CONTENT_1 > foo
  for num in `seq 1 6`
  do
    $RADOS_TOOL -p $POOL put foo_$num ./foo
  done

  # 3 Unique objects
  for num in `seq 7 9`
  do
    CONTENT_="There hiHI"$num
    echo $CONTENT_ > foo
    $RADOS_TOOL -p $POOL put foo_$num ./foo
  done

  # 6 dedupable objects
  CONTENT_2="There hiHIhi"
  echo $CONTENT_2 > foo
  for num in `seq 10 15`
  do
    $RADOS_TOOL -p $POOL put foo_$num ./foo
  done

  #Since the memory limit is 100 bytes, adding 3 unique objects causes a memory drop, leaving
  #the chunk of the 6 dupable objects. If we then add 6 dedupable objects to the pool,
  #the crawler should find dedupable chunks because it free memory space through the memory drop before.
  # 1 entry == 46 bytes

  sleep 2

  # Execute dedup crawler
  RESULT=$($DEDUP_DAEMON --pool $POOL --chunk-pool $CHUNK_POOL --chunk-algorithm fastcdc --fingerprint-algorithm sha1 --sampling-ratio 100 --chunk-dedup-threshold 2 --run-once)
  sleep 2
  PID=$(pidof ceph-dedup-daemon)
  COUNT=1
  while [ -n "$PID" ] && [ $COUNT -le 30 ]; do
    sleep 15
    PID=$(pidof ceph-dedup-daemon)
    ((COUNT++))
  done

  CHUNK_OID_1=$(echo $CONTENT_1 | sha1sum | awk '{print $1}')
  CHUNK_OID_2=$(echo $CONTENT_2 | sha1sum | awk '{print $1}')

  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID_1 | grep foo)
  if [ -z "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "There is no expected chunk object"
  fi

  RESULT=$($DEDUP_TOOL --op dump-chunk-refs --chunk-pool $CHUNK_POOL --object $CHUNK_OID_2 | grep foo)
  if [ -z "$RESULT" ] ; then
    $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
    $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
    die "There is no expected chunk object"
  fi

  rm -rf ./foo
  for num in `seq 1 15`
  do
    $RADOS_TOOL -p $POOL rm foo_$num
  done

  $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it
}

test_dedup_ratio_fixed
test_dedup_chunk_scrub
test_dedup_chunk_repair
test_dedup_object
test_sample_dedup
test_sample_dedup_snap
test_dedup_memory_limit

$CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it

echo "SUCCESS!"
exit 0
