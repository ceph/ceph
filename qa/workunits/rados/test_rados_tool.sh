#!/usr/bin/env bash

set -x

die() {
    echo "$@"
    exit 1
}

usage() {
    cat <<EOF
test_rados_tool.sh: tests rados_tool
-c:        RADOS configuration file to use [optional]
-k:        keep temp files
-h:        this help message
-p:        set temporary pool to use [optional]
EOF
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

run_expect_fail() {
    echo "RUN_EXPECT_FAIL: " "$@"
    do_run "$@"
    [ $? -eq 0 ] && die "expected failure, but got success! cmd: $@"
}

run_expect_succ() {
    echo "RUN_EXPECT_SUCC: " "$@"
    do_run "$@"
    [ $? -ne 0 ] && die "expected success, but got failure! cmd: $@"
}

run_expect_nosignal() {
    echo "RUN_EXPECT_NOSIGNAL: " "$@"
    do_run "$@"
    [ $? -ge 128 ] && die "expected succes or fail, but got signal! cmd: $@"
}

run() {
    echo "RUN: " $@
    do_run "$@"
}

if [ -n "$CEPH_BIN" ] ; then
   # CMake env
   RADOS_TOOL="$CEPH_BIN/rados"
   CEPH_TOOL="$CEPH_BIN/ceph"
else
   # executables should be installed by the QA env 
   RADOS_TOOL=$(which rados)
   CEPH_TOOL=$(which ceph)
fi

KEEP_TEMP_FILES=0
POOL=trs_pool
POOL_CP_TARGET=trs_pool.2
POOL_EC=trs_pool_ec

[ -x "$RADOS_TOOL" ] || die "couldn't find $RADOS_TOOL binary to test"
[ -x "$CEPH_TOOL" ] || die "couldn't find $CEPH_TOOL binary to test"

while getopts  "c:hkp:" flag; do
    case $flag in
        c)  RADOS_TOOL="$RADOS_TOOL -c $OPTARG";;
        k)  KEEP_TEMP_FILES=1;;
        h)  usage; exit 0;;
        p)  POOL=$OPTARG;;
        *)  echo; usage; exit 1;;
    esac
done

TDIR=`mktemp -d -t test_rados_tool.XXXXXXXXXX` || die "mktemp failed"
[ $KEEP_TEMP_FILES -eq 0 ] && trap "rm -rf ${TDIR}; exit" INT TERM EXIT

# ensure rados doesn't segfault without --pool
run_expect_nosignal "$RADOS_TOOL" --snap "asdf" ls
run_expect_nosignal "$RADOS_TOOL" --snapid "0" ls
run_expect_nosignal "$RADOS_TOOL" --object_locator "asdf" ls
run_expect_nosignal "$RADOS_TOOL" --namespace "asdf" ls

run_expect_succ "$RADOS_TOOL" mkpool "$POOL"
run_expect_succ "$CEPH_TOOL" osd erasure-code-profile set myprofile k=2 m=1 stripe_unit=2K crush-failure-domain=osd --force
run_expect_succ "$CEPH_TOOL" osd pool create "$POOL_EC" 100 100 erasure myprofile


# expb happens to be the empty export for legacy reasons
run_expect_succ "$RADOS_TOOL" -p "$POOL" export "$TDIR/expb"

# expa has objects foo, foo2 and bar
run_expect_succ "$RADOS_TOOL" -p "$POOL" put foo /etc/fstab
run_expect_succ "$RADOS_TOOL" -p "$POOL" put foo2 /etc/fstab
run_expect_succ "$RADOS_TOOL" -p "$POOL" put bar /etc/fstab
run_expect_succ "$RADOS_TOOL" -p "$POOL" export "$TDIR/expa"

# expc has foo and foo2 with some attributes and omaps set
run_expect_succ "$RADOS_TOOL" -p "$POOL" rm bar
run_expect_succ "$RADOS_TOOL" -p "$POOL" setxattr foo "rados.toothbrush" "toothbrush"
run_expect_succ "$RADOS_TOOL" -p "$POOL" setxattr foo "rados.toothpaste" "crest"
run_expect_succ "$RADOS_TOOL" -p "$POOL" setomapval foo "rados.floss" "myfloss"
run_expect_succ "$RADOS_TOOL" -p "$POOL" setxattr foo2 "rados.toothbrush" "green"
run_expect_succ "$RADOS_TOOL" -p "$POOL" setomapheader foo2 "foo2.header"
run_expect_succ "$RADOS_TOOL" -p "$POOL" export "$TDIR/expc"

# make sure that --create works
run "$RADOS_TOOL" rmpool "$POOL" "$POOL" --yes-i-really-really-mean-it
run_expect_succ "$RADOS_TOOL" -p "$POOL" --create import "$TDIR/expa"

# make sure that lack of --create fails
run_expect_succ "$RADOS_TOOL" rmpool "$POOL" "$POOL" --yes-i-really-really-mean-it
run_expect_fail "$RADOS_TOOL" -p "$POOL" import "$TDIR/expa"

run_expect_succ "$RADOS_TOOL" -p "$POOL" --create import "$TDIR/expa"

# inaccessible import src should fail
run_expect_fail "$RADOS_TOOL" -p "$POOL" import "$TDIR/dir_nonexistent"

# export an empty pool to test purge
run_expect_succ "$RADOS_TOOL" purge "$POOL" --yes-i-really-really-mean-it
run_expect_succ "$RADOS_TOOL" -p "$POOL" export "$TDIR/empty"
cmp -s "$TDIR/expb" "$TDIR/empty" \
    || die "failed to export the same stuff we imported!"
rm -f "$TDIR/empty"

# import some stuff with extended attributes on it
run_expect_succ "$RADOS_TOOL" -p "$POOL" import "$TDIR/expc"
VAL=`"$RADOS_TOOL" -p "$POOL" getxattr foo "rados.toothbrush"`
[ ${VAL} = "toothbrush" ] || die "Invalid attribute after import"

# the second time, the xattrs should match, so there should be nothing to do.
run_expect_succ "$RADOS_TOOL" -p "$POOL" import "$TDIR/expc"
VAL=`"$RADOS_TOOL" -p "$POOL" getxattr foo "rados.toothbrush"`
[ "${VAL}" = "toothbrush" ] || die "Invalid attribute after second import"

# Now try with --no-overwrite option after changing an attribute
run_expect_succ "$RADOS_TOOL" -p "$POOL" setxattr foo "rados.toothbrush" "dentist"
run_expect_succ "$RADOS_TOOL" -p "$POOL" import --no-overwrite "$TDIR/expc"
VAL=`"$RADOS_TOOL" -p "$POOL" getxattr foo "rados.toothbrush"`
[ "${VAL}" = "dentist" ] || die "Invalid attribute after second import"

# now force it to copy everything
run_expect_succ "$RADOS_TOOL" -p "$POOL" import "$TDIR/expc"
VAL=`"$RADOS_TOOL" -p "$POOL" getxattr foo "rados.toothbrush"`
[ "${VAL}" = "toothbrush" ] || die "Invalid attribute after second import"

# test copy pool
run "$RADOS_TOOL" rmpool "$POOL" "$POOL" --yes-i-really-really-mean-it
run "$RADOS_TOOL" rmpool "$POOL_CP_TARGET" "$POOL_CP_TARGET" --yes-i-really-really-mean-it
run_expect_succ "$RADOS_TOOL" mkpool "$POOL"
run_expect_succ "$RADOS_TOOL" mkpool "$POOL_CP_TARGET"

# create src files
mkdir -p "$TDIR/dir_cp_src"
for i in `seq 1 5`; do
  fname="$TDIR/dir_cp_src/f.$i"
  objname="f.$i"
  dd if=/dev/urandom of="$fname" bs=$((1024*1024)) count=$i
  run_expect_succ "$RADOS_TOOL" -p "$POOL" put $objname "$fname"

# a few random attrs
  for j in `seq 1 4`; do
    rand_str=`dd if=/dev/urandom bs=4 count=1 | hexdump -x`
    run_expect_succ "$RADOS_TOOL" -p "$POOL" setxattr $objname attr.$j "$rand_str"
    run_expect_succ --tee "$fname.attr.$j" "$RADOS_TOOL" -p "$POOL" getxattr $objname attr.$j
  done

  rand_str=`dd if=/dev/urandom bs=4 count=1 | hexdump -x`
  run_expect_succ "$RADOS_TOOL" -p "$POOL" setomapheader $objname "$rand_str"
  run_expect_succ --tee "$fname.omap.header" "$RADOS_TOOL" -p "$POOL" getomapheader $objname

# a few random omap keys
  for j in `seq 1 4`; do
    rand_str=`dd if=/dev/urandom bs=4 count=1 | hexdump -x`
    run_expect_succ "$RADOS_TOOL" -p "$POOL" setomapval $objname key.$j "$rand_str"
  done
  run_expect_succ --tee "$fname.omap.vals" "$RADOS_TOOL" -p "$POOL" listomapvals $objname
done

run_expect_succ "$RADOS_TOOL" cppool "$POOL" "$POOL_CP_TARGET"

mkdir -p "$TDIR/dir_cp_dst"
for i in `seq 1 5`; do
  fname="$TDIR/dir_cp_dst/f.$i"
  objname="f.$i"
  run_expect_succ "$RADOS_TOOL" -p "$POOL_CP_TARGET" get $objname "$fname"

# a few random attrs
  for j in `seq 1 4`; do
    run_expect_succ --tee "$fname.attr.$j" "$RADOS_TOOL" -p "$POOL_CP_TARGET" getxattr $objname attr.$j
  done

  run_expect_succ --tee "$fname.omap.header" "$RADOS_TOOL" -p "$POOL_CP_TARGET" getomapheader $objname
  run_expect_succ --tee "$fname.omap.vals" "$RADOS_TOOL" -p "$POOL_CP_TARGET" listomapvals $objname
done

diff -q -r "$TDIR/dir_cp_src" "$TDIR/dir_cp_dst" \
    || die "copy pool validation failed!"

for opt in \
    block-size \
    concurrent-ios \
    min-object-size \
    max-object-size \
    min-op-len \
    max-op-len \
    max-ops \
    max-backlog \
    target-throughput \
    read-percent \
    num-objects \
    run-length \
    ; do
    run_expect_succ "$RADOS_TOOL" --$opt 4 df
    run_expect_fail "$RADOS_TOOL" --$opt 4k df
done

run_expect_succ "$RADOS_TOOL" lock list f.1 --lock-duration 4 --pool "$POOL"
echo # previous command doesn't output an end of line: issue #9735
run_expect_fail "$RADOS_TOOL" lock list f.1 --lock-duration 4k --pool "$POOL"

run_expect_succ "$RADOS_TOOL" mksnap snap1 --pool "$POOL"
snapid=$("$RADOS_TOOL" lssnap --pool "$POOL" | grep snap1 | cut -f1)
[ $? -ne 0 ] && die "expected success, but got failure! cmd: \"$RADOS_TOOL\" lssnap --pool \"$POOL\" | grep snap1 | cut -f1"
run_expect_succ "$RADOS_TOOL" ls --pool "$POOL" --snapid="$snapid"
run_expect_fail "$RADOS_TOOL" ls --pool "$POOL" --snapid="$snapid"k

run_expect_succ "$RADOS_TOOL" chown 1 --pool "$POOL"
run_expect_fail "$RADOS_TOOL" chown 1k --pool "$POOL"

run_expect_succ "$RADOS_TOOL" truncate f.1 0 --pool "$POOL"
run_expect_fail "$RADOS_TOOL" truncate f.1 0k --pool "$POOL"

run "$RADOS_TOOL" rmpool delete_me_mkpool_test delete_me_mkpool_test --yes-i-really-really-mean-it
run_expect_succ "$RADOS_TOOL" mkpool delete_me_mkpool_test 0 0
run_expect_fail "$RADOS_TOOL" mkpool delete_me_mkpool_test2 0k 0
run_expect_fail "$RADOS_TOOL" mkpool delete_me_mkpool_test3 0 0k

run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 1 write
run_expect_fail "$RADOS_TOOL" --pool "$POOL" bench 1k write
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 1 write --format json --output "$TDIR/bench.json"
run_expect_fail "$RADOS_TOOL" --pool "$POOL" bench 1 write --output "$TDIR/bench.json"
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 5 write --format json --no-cleanup
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 1 rand --format json
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 1 rand -f json
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 1 seq --format json
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 1 seq -f json
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 5 write --write-omap
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 5 write --write-object
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 5 write --write-xattr
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 5 write --write-xattr --write-object
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 5 write --write-xattr --write-omap
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 5 write --write-omap --write-object
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 5 write --write-xattr --write-omap --write-object
run_expect_fail "$RADOS_TOOL" --pool "$POOL" bench 5 read --write-omap
run_expect_fail "$RADOS_TOOL" --pool "$POOL" bench 5 read --write-object
run_expect_fail "$RADOS_TOOL" --pool "$POOL" bench 5 read --write-xattr
run_expect_fail "$RADOS_TOOL" --pool "$POOL" bench 5 read --write-xattr --write-object
run_expect_fail "$RADOS_TOOL" --pool "$POOL" bench 5 read --write-xattr --write-omap
run_expect_fail "$RADOS_TOOL" --pool "$POOL" bench 5 read --write-omap --write-object
run_expect_fail "$RADOS_TOOL" --pool "$POOL" bench 5 read --write-xattr --write-omap --write-object

for i in $("$RADOS_TOOL" --pool "$POOL" ls | grep "benchmark_data"); do
    "$RADOS_TOOL" --pool "$POOL" truncate $i 0
done

run_expect_nosignal "$RADOS_TOOL" --pool "$POOL" bench 1 rand
run_expect_nosignal "$RADOS_TOOL" --pool "$POOL" bench 1 seq

set -e

OBJ=test_rados_obj

expect_false()
{
	if "$@"; then return 1; else return 0; fi
}

cleanup() {
    $RADOS_TOOL -p $POOL rm $OBJ > /dev/null 2>&1 || true
    $RADOS_TOOL -p $POOL_EC rm $OBJ > /dev/null 2>&1 || true
}

test_omap() {
    cleanup
    for i in $(seq 1 1 10)
    do
	if [ $(($i % 2)) -eq 0 ]; then
            $RADOS_TOOL -p $POOL setomapval $OBJ $i $i
	else
            echo -n "$i" | $RADOS_TOOL -p $POOL setomapval $OBJ $i
	fi
        $RADOS_TOOL -p $POOL getomapval $OBJ $i | grep -q "|$i|\$"
    done
    $RADOS_TOOL -p $POOL listomapvals $OBJ | grep -c value | grep 10
    for i in $(seq 1 1 5)
    do
        $RADOS_TOOL -p $POOL rmomapkey $OBJ $i
    done
    $RADOS_TOOL -p $POOL listomapvals $OBJ | grep -c value | grep 5
    $RADOS_TOOL -p $POOL clearomap $OBJ
    $RADOS_TOOL -p $POOL listomapvals $OBJ | wc -l | grep 0
    cleanup

    for i in $(seq 1 1 10)
    do
        dd if=/dev/urandom bs=128 count=1 > $TDIR/omap_key
        if [ $(($i % 2)) -eq 0 ]; then
            $RADOS_TOOL -p $POOL --omap-key-file $TDIR/omap_key setomapval $OBJ $i
        else
            echo -n "$i" | $RADOS_TOOL -p $POOL --omap-key-file $TDIR/omap_key setomapval $OBJ
        fi
        $RADOS_TOOL -p $POOL --omap-key-file $TDIR/omap_key getomapval $OBJ | grep -q "|$i|\$"
        $RADOS_TOOL -p $POOL --omap-key-file $TDIR/omap_key rmomapkey $OBJ
        $RADOS_TOOL -p $POOL listomapvals $OBJ | grep -c value | grep 0
    done
    cleanup
}

test_xattr() {
    cleanup
    $RADOS_TOOL -p $POOL put $OBJ /etc/passwd
    V1=`mktemp fooattrXXXXXXX`
    V2=`mktemp fooattrXXXXXXX`
    echo -n fooval > $V1
    expect_false $RADOS_TOOL -p $POOL setxattr $OBJ 2>/dev/null
    expect_false $RADOS_TOOL -p $POOL setxattr $OBJ foo fooval extraarg 2>/dev/null
    $RADOS_TOOL -p $POOL setxattr $OBJ foo fooval
    $RADOS_TOOL -p $POOL getxattr $OBJ foo > $V2
    cmp $V1 $V2
    cat $V1 | $RADOS_TOOL -p $POOL setxattr $OBJ bar
    $RADOS_TOOL -p $POOL getxattr $OBJ bar > $V2
    cmp $V1 $V2
    $RADOS_TOOL -p $POOL listxattr $OBJ > $V1
    grep -q foo $V1
    grep -q bar $V1
    [ `cat $V1 | wc -l` -eq 2 ]
    rm $V1 $V2
    cleanup
}
test_rmobj() {
    p=`uuidgen`
    $CEPH_TOOL osd pool create $p 1
    $CEPH_TOOL osd pool set-quota $p max_objects 1
    V1=`mktemp fooattrXXXXXXX`
    $RADOS_TOOL put $OBJ $V1 -p $p
    while ! $CEPH_TOOL osd dump | grep 'full_quota max_objects'
    do
	sleep 2
    done
    $RADOS_TOOL -p $p rm $OBJ --force-full
    $RADOS_TOOL rmpool $p $p --yes-i-really-really-mean-it
    rm $V1
}

test_ls() {
    echo "Testing rados ls command"
    p=`uuidgen`
    $CEPH_TOOL osd pool create $p 1
    NS=10
    OBJS=20
    # Include default namespace (0) in the total
    TOTAL=$(expr $OBJS \* $(expr $NS + 1))

    for nsnum in `seq 0 $NS`
    do
        for onum in `seq 1 $OBJS`
        do
	    if [ "$nsnum" = "0" ];
	    then
                "$RADOS_TOOL" -p $p put obj${onum} /etc/fstab 2> /dev/null
            else
                "$RADOS_TOOL" -p $p -N "NS${nsnum}" put obj${onum} /etc/fstab 2> /dev/null
	    fi
	done
    done
    CHECK=$("$RADOS_TOOL" -p $p ls 2> /dev/null | wc -l)
    if [ "$OBJS" -ne "$CHECK" ];
    then
        die "Created $OBJS objects in default namespace but saw $CHECK"
    fi
    TESTNS=NS${NS}
    CHECK=$("$RADOS_TOOL" -p $p -N $TESTNS ls 2> /dev/null | wc -l)
    if [ "$OBJS" -ne "$CHECK" ];
    then
        die "Created $OBJS objects in $TESTNS namespace but saw $CHECK"
    fi
    CHECK=$("$RADOS_TOOL" -p $p --all ls 2> /dev/null | wc -l)
    if [ "$TOTAL" -ne "$CHECK" ];
    then
        die "Created $TOTAL objects but saw $CHECK"
    fi

    $RADOS_TOOL rmpool $p $p --yes-i-really-really-mean-it
}

test_cleanup() {
    echo "Testing rados cleanup command"
    p=`uuidgen`
    $CEPH_TOOL osd pool create $p 1
    NS=5
    OBJS=4
    # Include default namespace (0) in the total
    TOTAL=$(expr $OBJS \* $(expr $NS + 1))

    for nsnum in `seq 0 $NS`
    do
        for onum in `seq 1 $OBJS`
        do
	    if [ "$nsnum" = "0" ];
	    then
                "$RADOS_TOOL" -p $p put obj${onum} /etc/fstab 2> /dev/null
            else
                "$RADOS_TOOL" -p $p -N "NS${nsnum}" put obj${onum} /etc/fstab 2> /dev/null
	    fi
	done
    done

    $RADOS_TOOL -p $p --all ls > $TDIR/before.ls.out 2> /dev/null

    $RADOS_TOOL -p $p bench 3 write --no-cleanup 2> /dev/null
    $RADOS_TOOL -p $p -N NS1 bench 3 write --no-cleanup 2> /dev/null
    $RADOS_TOOL -p $p -N NS2 bench 3 write --no-cleanup 2> /dev/null
    $RADOS_TOOL -p $p -N NS3 bench 3 write --no-cleanup 2> /dev/null
    # Leave dangling objects without a benchmark_last_metadata in NS4
    expect_false timeout 3 $RADOS_TOOL -p $p -N NS4 bench 30 write --no-cleanup 2> /dev/null
    $RADOS_TOOL -p $p -N NS5 bench 3 write --no-cleanup 2> /dev/null

    $RADOS_TOOL -p $p -N NS3 cleanup 2> /dev/null
    #echo "Check NS3 after specific cleanup"
    CHECK=$($RADOS_TOOL -p $p -N NS3 ls | wc -l)
    if [ "$OBJS" -ne "$CHECK" ] ;
    then
        die "Expected $OBJS objects in NS3 but saw $CHECK"
    fi

    #echo "Try to cleanup all"
    $RADOS_TOOL -p $p --all cleanup
    #echo "Check all namespaces"
    $RADOS_TOOL -p $p --all ls > $TDIR/after.ls.out 2> /dev/null
    CHECK=$(cat $TDIR/after.ls.out | wc -l)
    if [ "$TOTAL" -ne "$CHECK" ];
    then
        die "Expected $TOTAL objects but saw $CHECK"
    fi
    if ! diff $TDIR/before.ls.out $TDIR/after.ls.out
    then
        die "Different objects found after cleanup"
    fi

    set +e
    run_expect_fail $RADOS_TOOL -p $p cleanup --prefix illegal_prefix
    run_expect_succ $RADOS_TOOL -p $p cleanup --prefix benchmark_data_otherhost
    set -e

    $RADOS_TOOL rmpool $p $p --yes-i-really-really-mean-it
}

function test_append()
{
  cleanup

  # create object
  touch ./rados_append_null
  $RADOS_TOOL -p $POOL append $OBJ ./rados_append_null
  $RADOS_TOOL -p $POOL get $OBJ ./rados_append_0_out
  cmp ./rados_append_null ./rados_append_0_out

  # append 4k, total size 4k
  dd if=/dev/zero of=./rados_append_4k bs=4k count=1
  $RADOS_TOOL -p $POOL append $OBJ ./rados_append_4k
  $RADOS_TOOL -p $POOL get $OBJ ./rados_append_4k_out
  cmp ./rados_append_4k ./rados_append_4k_out

  # append 4k, total size 8k
  $RADOS_TOOL -p $POOL append $OBJ ./rados_append_4k
  $RADOS_TOOL -p $POOL get $OBJ ./rados_append_4k_out
  read_size=`ls -l ./rados_append_4k_out | awk -F ' '  '{print $5}'`
  if [ 8192 -ne $read_size ];
  then
    die "Append failed expecting 8192 read $read_size"
  fi

  # append 10M, total size 10493952
  dd if=/dev/zero of=./rados_append_10m bs=10M count=1
  $RADOS_TOOL -p $POOL append $OBJ ./rados_append_10m
  $RADOS_TOOL -p $POOL get $OBJ ./rados_append_10m_out
  read_size=`ls -l ./rados_append_10m_out | awk -F ' '  '{print $5}'`
  if [ 10493952 -ne $read_size ];
  then
    die "Append failed expecting 10493952 read $read_size"
  fi

  # cleanup
  cleanup

  # create object
  $RADOS_TOOL -p $POOL_EC append $OBJ ./rados_append_null
  $RADOS_TOOL -p $POOL_EC get $OBJ ./rados_append_0_out
  cmp rados_append_null rados_append_0_out

  # append 4k, total size 4k
  $RADOS_TOOL -p $POOL_EC append $OBJ ./rados_append_4k
  $RADOS_TOOL -p $POOL_EC get $OBJ ./rados_append_4k_out
  cmp rados_append_4k rados_append_4k_out

  # append 4k, total size 8k
  $RADOS_TOOL -p $POOL_EC append $OBJ ./rados_append_4k
  $RADOS_TOOL -p $POOL_EC get $OBJ ./rados_append_4k_out
  read_size=`ls -l ./rados_append_4k_out | awk -F ' '  '{print $5}'`
  if [ 8192 -ne $read_size ];
  then
    die "Append failed expecting 8192 read $read_size"
  fi

  # append 10M, total size 10493952
  $RADOS_TOOL -p $POOL_EC append $OBJ ./rados_append_10m
  $RADOS_TOOL -p $POOL_EC get $OBJ ./rados_append_10m_out
  read_size=`ls -l ./rados_append_10m_out | awk -F ' '  '{print $5}'`
  if [ 10493952 -ne $read_size ];
  then
    die "Append failed expecting 10493952 read $read_size"
  fi

  cleanup
  rm -rf ./rados_append_null ./rados_append_0_out
  rm -rf ./rados_append_4k ./rados_append_4k_out ./rados_append_10m ./rados_append_10m_out
}

function test_put()
{
  # rados put test:
  cleanup

  # create file in local fs
  dd if=/dev/urandom of=rados_object_10k bs=1K count=10

  # test put command
  $RADOS_TOOL -p $POOL put $OBJ ./rados_object_10k
  $RADOS_TOOL -p $POOL get $OBJ ./rados_object_10k_out
  cmp ./rados_object_10k ./rados_object_10k_out
  cleanup

  # test put command with offset 0
  $RADOS_TOOL -p $POOL put $OBJ ./rados_object_10k --offset 0
  $RADOS_TOOL -p $POOL get $OBJ ./rados_object_offset_0_out
  cmp ./rados_object_10k ./rados_object_offset_0_out
  cleanup

  # test put command with offset 1000
  $RADOS_TOOL -p $POOL put $OBJ ./rados_object_10k --offset 1000
  $RADOS_TOOL -p $POOL get $OBJ ./rados_object_offset_1000_out
  cmp ./rados_object_10k ./rados_object_offset_1000_out 0 1000
  cleanup

  rm -rf ./rados_object_10k ./rados_object_10k_out ./rados_object_offset_0_out ./rados_object_offset_1000_out
}

test_xattr
test_omap
test_rmobj
test_ls
test_cleanup
test_append
test_put

# clean up environment, delete pool
$CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
$CEPH_TOOL osd pool delete $POOL_EC $POOL_EC --yes-i-really-really-mean-it
$CEPH_TOOL osd pool delete $POOL_CP_TARGET $POOL_CP_TARGET --yes-i-really-really-mean-it

echo "SUCCESS!"
exit 0
