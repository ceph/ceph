#!/bin/bash

die() {
    echo "$@"
    exit 1
}

usage() {
    cat <<EOF
test_rados_sync.sh: tests rados_sync
-c:        RADOS configuration file to use [optional]
-k:        keep temp files
-h:        this help message
-p:        set temporary pool to use [optional]
EOF
}

run_expect_fail() {
    echo "RUN_EXPECT_FAIL: " $@
    $@
    [ $? -eq 0 ] && die "expected failure, but got success! cmd: $@"
}

run_expect_succ() {
    echo "RUN_EXPECT_SUCC: " $@
    $@
    [ $? -ne 0 ] && die "expected success, but got failure! cmd: $@"
}

run() {
    echo "RUN: " $@
    $@
}

DNAME="`dirname $0`"
DNAME="`readlink -f $DNAME`"
RADOS_SYNC="`readlink -f \"$DNAME/../rados_sync\"`"
RADOS_TOOL="`readlink -f \"$DNAME/../rados\"`"
KEEP_TEMP_FILES=0
POOL=trs_pool
[ -x "$RADOS_SYNC" ] || die "couldn't find $RADOS_SYNC binary to test"
[ -x "$RADOS_TOOL" ] || die "couldn't find $RADOS_TOOL binary"
which attr &>/dev/null
[ $? -eq 0 ] || die "you must install the 'attr' tool to manipulate \
extended attributes."

while getopts  "c:hkp:" flag; do
    case $flag in
        c)  RADOS_SYNC="$RADOS_SYNC -c $OPTARG";;
        k)  KEEP_TEMP_FILES=1;;
        h)  usage; exit 0;;
        p)  POOL=$OPTARG;;
        *)  echo; usage; exit 1;;
    esac
done

TDIR=`mktemp -d -t test_rados_sync.XXXXXXXXXX` || die "mktemp failed"
[ $KEEP_TEMP_FILES -eq 0 ] && trap "rm -rf ${TDIR}; exit" INT TERM EXIT

mkdir "$TDIR/dira"
touch "$TDIR/dira/000029c4_foo"
attr -q -s rados_full_name -V "foo" "$TDIR/dira/000029c4_foo"
touch "$TDIR/dira/00003036_foo2"
attr -q -s rados_full_name -V "foo2" "$TDIR/dira/00003036_foo2"
touch "$TDIR/dira/000027d5_bar"
attr -q -s rados_full_name -V "bar" "$TDIR/dira/000027d5_bar"
mkdir "$TDIR/dirb"

# make sure that --create works
run "$RADOS_TOOL" rmpool "$POOL"
run_expect_succ "$RADOS_SYNC" --create import "$TDIR/dira" "$POOL"

# make sure that lack of --create fails
run_expect_succ "$RADOS_TOOL" rmpool "$POOL"
run_expect_fail "$RADOS_SYNC" import "$TDIR/dira" "$POOL"

# export some stuff
run_expect_succ "$RADOS_SYNC" --create import "$TDIR/dira" "$POOL"
run_expect_succ "$RADOS_SYNC" --create export "$POOL" "$TDIR/dirb"
diff -q -r "$TDIR/dira" "$TDIR/dirb" \
    || die "failed to export the same stuff we imported!"

echo "SUCCESS!"
exit 0
