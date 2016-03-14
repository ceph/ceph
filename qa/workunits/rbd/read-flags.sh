#!/bin/bash -ex

# create a snapshot, then export it and check that setting read flags works
# by looking at --debug-ms output

function clean_up {
    rm -f test.log || true
    rbd snap remove test@snap || true
    rbd rm test || true
}

function test_read_flags {
    local IMAGE=$1
    local SET_BALANCED=$2
    local SET_LOCALIZED=$3
    local EXPECT_BALANCED=$4
    local EXPECT_LOCALIZED=$5

    local EXTRA_ARGS="--log-file test.log --debug-ms 1 --no-log-to-stderr"
    if [ "$SET_BALANCED" = 'y' ]; then
	EXTRA_ARGS="$EXTRA_ARGS --rbd-balance-snap-reads"
    elif [ "$SET_LOCALIZED" = 'y' ]; then
	EXTRA_ARGS="$EXTRA_ARGS --rbd-localize-snap-reads"
    fi

    rbd export $IMAGE - $EXTRA_ARGS > /dev/null
    if [ "$EXPECT_BALANCED" = 'y' ]; then
	grep -q balance_reads test.log
    else
	grep -L balance_reads test.log | grep -q test.log
    fi
    if [ "$EXPECT_LOCALIZED" = 'y' ]; then
	grep -q localize_reads test.log
    else
	grep -L localize_reads test.log | grep -q test.log
    fi
    rm -f test.log

}

clean_up

trap clean_up INT TERM EXIT

rbd create --image-feature layering -s 10 test
rbd snap create test@snap

# export from non snapshot with or without settings should not have flags
test_read_flags test n n n n
test_read_flags test y y n n

# export from snapshot should have read flags in log if they are set
test_read_flags test@snap n n n n
test_read_flags test@snap y n y n
test_read_flags test@snap n y n y

# balanced_reads happens to take priority over localize_reads
test_read_flags test@snap y y y n

echo OK
