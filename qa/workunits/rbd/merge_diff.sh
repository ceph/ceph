#!/bin/bash -ex

pool=rbd
gen=$pool/gen
out=$pool/out
testno=1

mkdir -p merge_diff_test
pushd merge_diff_test

function expect_false()
{
  if "$@"; then return 1; else return 0; fi
}

function clear_all()
{
  fusermount -u mnt || true

  rbd snap purge --no-progress $gen || true
  rbd rm --no-progress $gen || true
  rbd snap purge --no-progress $out || true
  rbd rm --no-progress $out || true

  rm -rf diffs || true
}

function rebuild()
{
  clear_all
  echo Starting test $testno
  ((testno++))
  if [[ "$2" -lt "$1" ]] && [[ "$3" -gt "1" ]]; then
    rbd create $gen --size 100 --object-size $1 --stripe-unit $2 --stripe-count $3 --image-format $4
  else
    rbd create $gen --size 100 --object-size $1 --image-format $4
  fi
  rbd create $out --size 1 --object-size 524288
  mkdir -p mnt diffs
  # lttng has atexit handlers that need to be fork/clone aware
  LD_PRELOAD=liblttng-ust-fork.so.0 rbd-fuse -p $pool mnt
}

function write()
{
  dd if=/dev/urandom of=mnt/gen bs=1M conv=notrunc seek=$1 count=$2
}

function snap()
{
  rbd snap create $gen@$1
}

function resize()
{
  rbd resize --no-progress $gen --size $1 --allow-shrink
}

function export_diff()
{
  if [ $2 == "head" ]; then
    target="$gen"
  else
    target="$gen@$2"
  fi
  if [ $1 == "null" ]; then
    rbd export-diff --no-progress $target diffs/$1.$2
  else
    rbd export-diff --no-progress $target --from-snap $1 diffs/$1.$2
  fi
}

function merge_diff()
{
  rbd merge-diff diffs/$1.$2 diffs/$2.$3 diffs/$1.$3
}

function check()
{
  rbd import-diff --no-progress diffs/$1.$2 $out || return -1
  if [ "$2" == "head" ]; then
    sum1=`rbd export $gen - | md5sum`
  else
    sum1=`rbd export $gen@$2 - | md5sum`
  fi
  sum2=`rbd export $out - | md5sum`
  if [ "$sum1" != "$sum2" ]; then
    exit -1
  fi
  if [ "$2" != "head" ]; then
    rbd snap ls $out | awk '{print $2}' | grep "^$2\$" || return -1
  fi
}

#test f/t header
rebuild 4194304 4194304 1 2
write 0 1
snap a
write 1 1
export_diff null a
export_diff a head
merge_diff null a head
check null head

rebuild 4194304 4194304 1 2
write 0 1
snap a
write 1 1
snap b
write 2 1
export_diff null a
export_diff a b
export_diff b head
merge_diff null a b
check null b

rebuild 4194304 4194304 1 2
write 0 1
snap a
write 1 1
snap b
write 2 1
export_diff null a
export_diff a b
export_diff b head
merge_diff a b head
check null a
check a head

rebuild 4194304 4194304 1 2
write 0 1
snap a
write 1 1
snap b
write 2 1
export_diff null a
export_diff a b
export_diff b head
rbd merge-diff diffs/null.a diffs/a.b - | rbd merge-diff - diffs/b.head - > diffs/null.head
check null head

#data test
rebuild 4194304 4194304 1 2
write 4 2
snap s101
write 0 3
write 8 2
snap s102
export_diff null s101
export_diff s101 s102
merge_diff null s101 s102
check null s102

rebuild 4194304 4194304 1 2
write 0 3
write 2 5
write 8 2
snap s201
write 0 2
write 6 3
snap s202
export_diff null s201
export_diff s201 s202
merge_diff null s201 s202
check null s202

rebuild 4194304 4194304 1 2
write 0 4
write 12 6
snap s301
write 0 6
write 10 5
write 16 4
snap s302
export_diff null s301
export_diff s301 s302
merge_diff null s301 s302
check null s302

rebuild 4194304 4194304 1 2
write 0 12
write 14 2
write 18 2
snap s401
write 1 2
write 5 6
write 13 3
write 18 2
snap s402
export_diff null s401
export_diff s401 s402
merge_diff null s401 s402
check null s402

rebuild 4194304 4194304 1 2
write 2 4
write 10 12
write 27 6
write 36 4
snap s501
write 0 24
write 28 4
write 36 4
snap s502
export_diff null s501
export_diff s501 s502
merge_diff null s501 s502
check null s502

rebuild 4194304 4194304 1 2
write 0 8
resize 5
snap r1
resize 20
write 12 8
snap r2
resize 8
write 4 4
snap r3
export_diff null r1
export_diff r1 r2
export_diff r2 r3
merge_diff null r1 r2
merge_diff null r2 r3
check null r3

rebuild 4194304 4194304 1 2
write 0 8
resize 5
snap r1
resize 20
write 12 8
snap r2
resize 8
write 4 4
snap r3
resize 10
snap r4
export_diff null r1
export_diff r1 r2
export_diff r2 r3
export_diff r3 r4
merge_diff null r1 r2
merge_diff null r2 r3
merge_diff null r3 r4
check null r4

# merge diff doesn't yet support fancy striping
# rebuild 4194304 65536 8 2
# write 0 32
# snap r1
# write 16 32
# snap r2
# export_diff null r1
# export_diff r1 r2
# expect_false merge_diff null r1 r2

rebuild 4194304 4194304 1 2
write 0 1
write 2 1
write 4 1
write 6 1
snap s1
write 1 1
write 3 1
write 5 1
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 1 1
write 3 1
write 5 1
snap s1
write 0 1
write 2 1
write 4 1
write 6 1
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 0 3
write 6 3
write 12 3
snap s1
write 1 1
write 7 1
write 13 1
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 0 3
write 6 3
write 12 3
snap s1
write 0 1
write 6 1
write 12 1
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 0 3
write 6 3
write 12 3
snap s1
write 2 1
write 8 1
write 14 1
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 1 1
write 7 1
write 13 1
snap s1
write 0 3
write 6 3
write 12 3
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 0 1
write 6 1
write 12 1
snap s1
write 0 3
write 6 3
write 12 3
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 2 1
write 8 1
write 14 1
snap s1
write 0 3
write 6 3
write 12 3
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 0 3
write 6 3
write 12 3
snap s1
write 0 3
write 6 3
write 12 3
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 2 4
write 8 4
write 14 4
snap s1
write 0 3
write 6 3
write 12 3
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 0 4
write 6 4
write 12 4
snap s1
write 0 3
write 6 3
write 12 3
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 0 6
write 6 6
write 12 6
snap s1
write 0 3
write 6 3
write 12 3
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 3 6
write 9 6
write 15 6
snap s1
write 0 3
write 6 3
write 12 3
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 0 8
snap s1
resize 2
resize 100
snap s2
export_diff null s1
export_diff s1 s2
merge_diff null s1 s2
check null s2

rebuild 4194304 4194304 1 2
write 0 8
snap s1
resize 2
resize 100
snap s2
write 20 2
snap s3
export_diff null s1
export_diff s1 s2
export_diff s2 s3
merge_diff s1 s2 s3
check null s1
check s1 s3

#addme

clear_all
popd
rm -rf merge_diff_test

echo OK
