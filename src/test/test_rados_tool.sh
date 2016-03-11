#!/bin/bash

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

run() {
    echo "RUN: " $@
    do_run "$@"
}

DNAME="`dirname $0`"
DNAME="`readlink -f $DNAME`"
RADOS_TOOL="`readlink -f \"$DNAME/../rados\"`"
KEEP_TEMP_FILES=0
POOL=trs_pool
POOL_CP_TARGET=trs_pool.2

[ -x "$RADOS_TOOL" ] || die "couldn't find $RADOS_TOOL binary to test"
[ -x "$RADOS_TOOL" ] || die "couldn't find $RADOS_TOOL binary"
which attr &>/dev/null
[ $? -eq 0 ] || die "you must install the 'attr' tool to manipulate \
extended attributes."

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

mkdir "$TDIR/dira"
attr -q -s "rados_sync_ver" -V "1" "$TDIR/dira"
touch "$TDIR/dira/foo"
attr -q -s "rados_full_name" -V "foo" "$TDIR/dira/foo"
touch "$TDIR/dira/foo2"
attr -q -s "rados_full_name" -V "foo2" "$TDIR/dira/foo2"
touch "$TDIR/dira/bar"
attr -q -s "rados_full_name" -V "bar" "$TDIR/dira/bar"
mkdir "$TDIR/dirb"
attr -q -s "rados_sync_ver" -V "1" "$TDIR/dirb"
mkdir "$TDIR/dirc"
attr -q -s "rados_sync_ver" -V "1" "$TDIR/dirc"
touch "$TDIR/dirc/foo"
attr -q -s "rados_full_name" -V "foo" "$TDIR/dirc/foo"
attr -q -s "rados.toothbrush" -V "toothbrush" "$TDIR/dirc/foo"
attr -q -s "rados.toothpaste" -V "crest" "$TDIR/dirc/foo"
attr -q -s "rados.floss" -V "myfloss" "$TDIR/dirc/foo"
touch "$TDIR/dirc/foo2"
attr -q -s "rados.toothbrush" -V "green" "$TDIR/dirc/foo2"
attr -q -s "rados_full_name" -V "foo2" "$TDIR/dirc/foo2"

# make sure that --create works
run "$RADOS_TOOL" rmpool "$POOL" "$POOL" --yes-i-really-really-mean-it
run_expect_succ "$RADOS_TOOL" --create import "$TDIR/dira" "$POOL"

# make sure that lack of --create fails
run_expect_succ "$RADOS_TOOL" rmpool "$POOL" "$POOL" --yes-i-really-really-mean-it
run_expect_fail "$RADOS_TOOL" import "$TDIR/dira" "$POOL"

run_expect_succ "$RADOS_TOOL" --create import "$TDIR/dira" "$POOL"

# inaccessible import src should fail
run_expect_fail "$RADOS_TOOL" import "$TDIR/dir_nonexistent" "$POOL"

# export some stuff
run_expect_succ "$RADOS_TOOL" --create export "$POOL" "$TDIR/dirb"
diff -q -r "$TDIR/dira" "$TDIR/dirb" \
    || die "failed to export the same stuff we imported!"

# import some stuff with extended attributes on it
run_expect_succ "$RADOS_TOOL" import "$TDIR/dirc" "$POOL" | tee "$TDIR/out"
run_expect_succ grep -q '\[xattr\]' $TDIR/out

# the second time, the xattrs should match, so there should be nothing to do.
run_expect_succ "$RADOS_TOOL" import "$TDIR/dirc" "$POOL" | tee "$TDIR/out"
run_expect_fail grep -q '\[xattr\]' "$TDIR/out"

# now force it to copy everything
run_expect_succ "$RADOS_TOOL" --force import "$TDIR/dirc" "$POOL" | tee "$TDIR/out2"
run_expect_succ grep '\[force\]' "$TDIR/out2"

# export some stuff with extended attributes on it
run_expect_succ "$RADOS_TOOL" -C export "$POOL" "$TDIR/dirc_copy"

# check to make sure extended attributes were preserved
PRE_EXPORT=`attr -qg rados.toothbrush "$TDIR/dirc/foo"`
[ $? -eq 0 ] || die "failed to get xattr"
POST_EXPORT=`attr -qg rados.toothbrush "$TDIR/dirc_copy/foo"`
[ $? -eq 0 ] || die "failed to get xattr"
if [ "$PRE_EXPORT" != "$POST_EXPORT" ]; then
    die "xattr not preserved across import/export! \
\$PRE_EXPORT = $PRE_EXPORT, \$POST_EXPORT = $POST_EXPORT"
fi

# another try with a different --worker setting
run_expect_succ "$RADOS_TOOL" --workers 1 -C export "$POOL" "$TDIR/dirc_copy2"

# another try with a different --worker setting
run_expect_succ "$RADOS_TOOL" --workers 30 -C export "$POOL" "$TDIR/dirc_copy3"

# trigger a rados delete using --delete-after
run_expect_succ "$RADOS_TOOL" --create export "$POOL" "$TDIR/dird"
rm -f "$TDIR/dird/foo"
run_expect_succ "$RADOS_TOOL" --delete-after import "$TDIR/dird" "$POOL" | tee "$TDIR/out3"
run_expect_succ grep '\[deleted\]' "$TDIR/out3"

# trigger a local delete using --delete-after
run_expect_succ "$RADOS_TOOL" --delete-after export "$POOL" "$TDIR/dirc" | tee "$TDIR/out4"
run_expect_succ grep '\[deleted\]' "$TDIR/out4"
[ -e "$TDIR/dird/foo" ] && die "--delete-after failed to delete a file!"

# test hashed pathnames
mkdir -p "$TDIR/dird"
attr -q -s "rados_sync_ver" -V "1" "$TDIR/dird"
touch "$TDIR/dird/bar@bar_00000000000055ca"
attr -q -s "rados_full_name" -V "bar/bar" "$TDIR/dird/bar@bar_00000000000055ca"
run_expect_succ "$RADOS_TOOL" --delete-after import "$TDIR/dird" "$POOL" | tee "$TDIR/out5"
run_expect_succ grep '\[imported\]' "$TDIR/out5"
run_expect_succ "$RADOS_TOOL" --delete-after --create export "$POOL" "$TDIR/dire" | tee "$TDIR/out6"
run_expect_succ grep '\[exported\]' "$TDIR/out6"
diff -q -r "$TDIR/dird" "$TDIR/dire" \
    || die "failed to export the same stuff we imported!"

# create a temporary file and validate that export deletes it
touch "$TDIR/dire/tmp\$tmp"
run_expect_succ "$RADOS_TOOL" --delete-after --create export "$POOL" "$TDIR/dire" | tee "$TDIR/out7"
run_expect_succ grep temporary "$TDIR/out7"


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
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 1 rand
run_expect_succ "$RADOS_TOOL" --pool "$POOL" bench 1 seq


echo "SUCCESS!"
exit 0
