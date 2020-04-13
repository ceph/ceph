#!/usr/bin/env bash

# - fallocate -z deallocates because BLKDEV_ZERO_NOUNMAP hint is ignored by
# krbd
#
# - big unaligned blkdiscard and fallocate -z/-p leave the objects in place

set -ex

# no blkdiscard(8) in trusty
function py_blkdiscard() {
    local offset=$1

    python <<EOF
import fcntl, struct
BLKDISCARD = 0x1277
with open('$DEV', 'w') as dev:
    fcntl.ioctl(dev, BLKDISCARD, struct.pack('QQ', $offset, $IMAGE_SIZE - $offset))
EOF
}

# fallocate(1) in trusty doesn't support -z/-p
function py_fallocate() {
    local mode=$1
    local offset=$2

    python <<EOF
import os, ctypes, ctypes.util
FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02
FALLOC_FL_ZERO_RANGE = 0x10
libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
with open('$DEV', 'w') as dev:
    if libc.fallocate(dev.fileno(), ctypes.c_int($mode), ctypes.c_long($offset), ctypes.c_long($IMAGE_SIZE - $offset)):
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
EOF
}

function allocate() {
    xfs_io -c "pwrite -b $OBJECT_SIZE -W 0 $IMAGE_SIZE" $DEV
    assert_allocated
}

function assert_allocated() {
    cmp <(od -xAx $DEV) - <<EOF
000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
*
$(printf %x $IMAGE_SIZE)
EOF
    [[ $(rados -p rbd ls | grep -c rbd_data.$IMAGE_ID) -eq $NUM_OBJECTS ]]
}

function assert_zeroes() {
    local num_objects_expected=$1

    cmp <(od -xAx $DEV) - <<EOF
000000 0000 0000 0000 0000 0000 0000 0000 0000
*
$(printf %x $IMAGE_SIZE)
EOF
    [[ $(rados -p rbd ls | grep -c rbd_data.$IMAGE_ID) -eq $num_objects_expected ]]
}

function assert_zeroes_unaligned() {
    local num_objects_expected=$1

    cmp <(od -xAx $DEV) - <<EOF
000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
*
$(printf %x $((OBJECT_SIZE / 2))) 0000 0000 0000 0000 0000 0000 0000 0000
*
$(printf %x $IMAGE_SIZE)
EOF
    [[ $(rados -p rbd ls | grep -c rbd_data.$IMAGE_ID) -eq $num_objects_expected ]]
    for ((i = 0; i < $num_objects_expected; i++)); do
        rados -p rbd stat rbd_data.$IMAGE_ID.$(printf %016x $i) | egrep "(size $((OBJECT_SIZE / 2)))|(size 0)"
    done
}

IMAGE_NAME="fallocate-test"

rbd create --size 200 $IMAGE_NAME

IMAGE_SIZE=$(rbd info --format=json $IMAGE_NAME | python -c 'import sys, json; print json.load(sys.stdin)["size"]')
OBJECT_SIZE=$(rbd info --format=json $IMAGE_NAME | python -c 'import sys, json; print json.load(sys.stdin)["object_size"]')
NUM_OBJECTS=$((IMAGE_SIZE / OBJECT_SIZE))
[[ $((IMAGE_SIZE % OBJECT_SIZE)) -eq 0 ]]

IMAGE_ID="$(rbd info --format=json $IMAGE_NAME |
    python -c "import sys, json; print json.load(sys.stdin)['block_name_prefix'].split('.')[1]")"

DEV=$(sudo rbd map $IMAGE_NAME)

# make sure -ENOENT is hidden
assert_zeroes 0
py_blkdiscard 0
assert_zeroes 0

# blkdev_issue_discard
allocate
py_blkdiscard 0
assert_zeroes 0

# blkdev_issue_zeroout w/ BLKDEV_ZERO_NOUNMAP
allocate
py_fallocate FALLOC_FL_ZERO_RANGE\|FALLOC_FL_KEEP_SIZE 0
assert_zeroes 0

# blkdev_issue_zeroout w/ BLKDEV_ZERO_NOFALLBACK
allocate
py_fallocate FALLOC_FL_PUNCH_HOLE\|FALLOC_FL_KEEP_SIZE 0
assert_zeroes 0

# unaligned blkdev_issue_discard
allocate
py_blkdiscard $((OBJECT_SIZE / 2))
assert_zeroes_unaligned $NUM_OBJECTS

# unaligned blkdev_issue_zeroout w/ BLKDEV_ZERO_NOUNMAP
allocate
py_fallocate FALLOC_FL_ZERO_RANGE\|FALLOC_FL_KEEP_SIZE $((OBJECT_SIZE / 2))
assert_zeroes_unaligned $NUM_OBJECTS

# unaligned blkdev_issue_zeroout w/ BLKDEV_ZERO_NOFALLBACK
allocate
py_fallocate FALLOC_FL_PUNCH_HOLE\|FALLOC_FL_KEEP_SIZE $((OBJECT_SIZE / 2))
assert_zeroes_unaligned $NUM_OBJECTS

sudo rbd unmap $DEV

DEV=$(sudo rbd map -o notrim $IMAGE_NAME)

# blkdev_issue_discard
allocate
py_blkdiscard 0 |& grep 'Operation not supported'
assert_allocated

# blkdev_issue_zeroout w/ BLKDEV_ZERO_NOUNMAP
allocate
py_fallocate FALLOC_FL_ZERO_RANGE\|FALLOC_FL_KEEP_SIZE 0
assert_zeroes $NUM_OBJECTS

# blkdev_issue_zeroout w/ BLKDEV_ZERO_NOFALLBACK
allocate
py_fallocate FALLOC_FL_PUNCH_HOLE\|FALLOC_FL_KEEP_SIZE 0 |& grep 'Operation not supported'
assert_allocated

sudo rbd unmap $DEV

echo OK
