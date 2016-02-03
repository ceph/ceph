#!/bin/sh -ex

if ! id -u | grep -q '^0$'; then
    echo "not root, re-running self via sudo"
    sudo PATH=$PATH $0 || echo FAIL
    exit 0
fi

function expect_false()
{
        set -x
        if "$@"; then return 1; else return 0; fi
}

COT=ceph-objectstore-tool
DATA=store_test_fuse_dir
TYPE=bluestore
MNT=store_test_fuse_mnt

rm -rf $DATA
mkdir -p $DATA

fusermount -u $MNT || true
rmdir $MNT || true
mkdir $MNT

$COT --op mkfs --data-path $DATA --type $TYPE
$COT --op fuse --data-path $DATA --mountpoint $MNT &

while ! test -e $MNT/type ; do
    echo waiting for $MNT/type to appear
    sleep 1
done

umask 0

grep $TYPE $MNT/type

# create collection
mkdir $MNT/meta
test -e $MNT/meta/bitwise_hash_start
test -d $MNT/meta/all
test -d $MNT/meta/by_bitwise_hash

# create object
mkdir $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@
test -e $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/data
test -d $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/attr
test -d $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap
test -e $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/bitwise_hash
test -e $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap_header

# omap header
echo omap header > $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap_header
grep -q omap $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap_header

# omap
echo value a > $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap/keya
echo value b > $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap/keyb
ls $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap | grep -c key | grep -q 2
grep 'value a' $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap/keya
grep 'value b' $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap/keyb
rm $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap/keya
test ! -e $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap/keya
rm $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap/keyb
test ! -e $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/omap/keyb

# attr
echo value a > $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/attr/keya
echo value b > $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/attr/keyb
ls $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/attr | grep -c key | grep -q 2
grep 'value a' $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/attr/keya
grep 'value b' $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/attr/keyb
rm $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/attr/keya
test ! -e $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/attr/keya
rm $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/attr/keyb
test ! -e $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/attr/keyb

# data
test ! -s $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/data
echo asdfasdfasdf > $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/data
test -s $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/data
grep -q asdfasdfasdf $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/data
rm $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/data
test ! -s $MNT/meta/all/@-1:7b3f43c4:::osd_superblock:0@/data

# create pg collection
mkdir --mode 0003 $MNT/0.0_head
cat $MNT/0.0_head/bitwise_hash_bits
grep -q 3 $MNT/0.0_head/bitwise_hash_bits
grep -q 00000000 $MNT/0.0_head/bitwise_hash_start
grep -q 1fffffff $MNT/0.0_head/bitwise_hash_end
test -d $MNT/0.0_head/all

mkdir --mode 0003 $MNT/0.1_head
grep -q 3 $MNT/0.1_head/bitwise_hash_bits
grep -q 80000000 $MNT/0.1_head/bitwise_hash_start
grep -q 9fffffff $MNT/0.1_head/bitwise_hash_end

# create pg object
mkdir $MNT/0.0_head/all/@0:00000000::::head@/
mkdir $MNT/0.0_head/all/@0:10000000:::foo:head@/

# verify pg bounds check
expect_false mkdir $MNT/0.0_head/all/@0:20000000:::bar:head@/

# remove a collection
expect_false rmdir $MNT/0.0_head
rmdir $MNT/0.0_head/all/@0:10000000:::foo:head@/
rmdir $MNT/0.0_head/all/@0:00000000::::head@/
rmdir $MNT/0.0_head
rmdir $MNT/0.1_head

fusermount -u $MNT
wait

echo OK
