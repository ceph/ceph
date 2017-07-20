#!/usr/bin/env bash
set -x

#
# Add some objects to the data PGs, and then test splitting those PGs
#

# Includes
source "`dirname $0`/test_common.sh"

TEST_POOL=rbd

# Constants
my_write_objects() {
        write_objects $1 $2 10 1000000 $TEST_POOL
}

setup() {
        export CEPH_NUM_OSD=$1

        # Start ceph
        ./stop.sh

        ./vstart.sh -d -n
}

get_pgp_num() {
        ./ceph -c ./ceph.conf osd pool get $TEST_POOL pgp_num > $TEMPDIR/pgp_num
        [ $? -eq 0 ] || die "failed to get pgp_num"
        PGP_NUM=`grep PGP_NUM $TEMPDIR/pgp_num | sed 's/.*PGP_NUM:\([ 0123456789]*\).*$/\1/'`
}

split1_impl() {
        # Write lots and lots of objects
        my_write_objects 1 2

        get_pgp_num
        echo "\$PGP_NUM=$PGP_NUM"

        # Double the number of PGs
        PGP_NUM=$((PGP_NUM*2))
        echo "doubling PGP_NUM to $PGP_NUM..."
        ./ceph -c ./ceph.conf osd pool set $TEST_POOL pgp_num $PGP_NUM

        sleep 30

        # success
        return 0
}

split1() {
        setup 2
        split1_impl
}

many_pools() {
        setup 3
        for i in `seq 1 3000`; do
                ./rados -c ./ceph.conf mkpool "pool${i}" || die "mkpool failed"
        done
        my_write_objects 1 10
}

run() {
        split1 || die "test failed"
}

$@
