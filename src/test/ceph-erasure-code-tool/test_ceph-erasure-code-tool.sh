#!/bin/sh -ex

TMPDIR=/tmp/test_ceph-erasure-code-tool.$$
mkdir $TMPDIR
trap "rm -fr $TMPDIR" 0

ceph-erasure-code-tool test-plugin-exists INVALID_PLUGIN && exit 1
ceph-erasure-code-tool test-plugin-exists isa

ceph-erasure-code-tool validate-profile \
                       plugin=isa,technique=reed_sol_van,k=2,m=1

test "$(ceph-erasure-code-tool validate-profile \
          plugin=isa,technique=reed_sol_van,k=2,m=1 chunk_count)" = 3

test "$(ceph-erasure-code-tool calc-chunk-size \
          plugin=isa,technique=reed_sol_van,k=2,m=1 4194304)" = 2097152

dd if="$(which ceph-erasure-code-tool)" of=$TMPDIR/data bs=770808 count=1
cp $TMPDIR/data $TMPDIR/data.orig

# cauchy_orig needs k*w*packetsize aligned chunks
ceph-erasure-code-tool encode \
                       plugin=jerasure,technique=cauchy_orig,k=2,m=1 \
                       4096 \
                       0,1,2 \
                       $TMPDIR/data 2>&1 |
    grep -q 'invalid stripe unit'

ceph-erasure-code-tool encode \
                       plugin=isa,technique=reed_sol_van,k=2,m=1 \
                       4096 \
                       0,1,2 \
                       $TMPDIR/data
test -f $TMPDIR/data.0
test -f $TMPDIR/data.1
test -f $TMPDIR/data.2

rm $TMPDIR/data

ceph-erasure-code-tool decode \
                       plugin=isa,technique=reed_sol_van,k=2,m=1 \
                       4096 \
                       0,2 \
                       $TMPDIR/data

size=$(stat -c '%s' $TMPDIR/data.orig)
truncate -s "${size}" $TMPDIR/data # remove stripe width padding
cmp $TMPDIR/data.orig $TMPDIR/data

# decode without the first shard, and without the one holding the tail
dd if=$TMPDIR/data.orig of=$TMPDIR/small.orig bs=8192 count=3
cp $TMPDIR/small.orig $TMPDIR/small
ceph-erasure-code-tool encode \
                       plugin=isa,technique=reed_sol_van,k=2,m=1 \
                       4096 \
                       0,1,2 \
                       $TMPDIR/small
for shards in 1,2 0,2; do
    rm $TMPDIR/small
    ceph-erasure-code-tool decode \
                           plugin=isa,technique=reed_sol_van,k=2,m=1 \
                           4096 \
                           $shards \
                           $TMPDIR/small
    cmp $TMPDIR/small.orig $TMPDIR/small
done

# lrc maps data chunks to shards other than 0..k-1
rm $TMPDIR/data.*[0-9]
ceph-erasure-code-tool encode \
                       plugin=lrc,k=4,m=2,l=3 \
                       4096 \
                       0,1,2,3,4,5,6,7 \
                       $TMPDIR/data
rm $TMPDIR/data
ceph-erasure-code-tool decode \
                       plugin=lrc,k=4,m=2,l=3 \
                       4096 \
                       0,1,2,3,4,5,6,7 \
                       $TMPDIR/data
truncate -s "${size}" $TMPDIR/data
cmp $TMPDIR/data.orig $TMPDIR/data

echo OK
