#!/usr/bin/env bash

set -ex

function assert_dm() {
    local name=$1
    local val=$2

    local devno
    devno=$(sudo dmsetup info -c --noheadings -o Major,Minor $name)
    grep -q $val /sys/dev/block/$devno/bdi/stable_pages_required
}

function dmsetup_reload() {
    local name=$1

    local table
    table=$(</dev/stdin)

    sudo dmsetup suspend $name
    echo "$table" | sudo dmsetup reload $name
    sudo dmsetup resume $name
}

IMAGE_NAME="stable-pages-required-test"

rbd create --size 1 $IMAGE_NAME
DEV=$(sudo rbd map $IMAGE_NAME)

fallocate -l 1M loopfile
LOOP_DEV=$(sudo losetup -f --show loopfile)

[[ $(blockdev --getsize64 $DEV) -eq 1048576 ]]
grep -q 1 /sys/block/${DEV#/dev/}/bdi/stable_pages_required

rbd resize --size 2 $IMAGE_NAME
[[ $(blockdev --getsize64 $DEV) -eq 2097152 ]]
grep -q 1 /sys/block/${DEV#/dev/}/bdi/stable_pages_required

cat <<EOF | sudo dmsetup create tbl
0 1024 linear $LOOP_DEV 0
EOF
assert_dm tbl 0
sudo dmsetup remove tbl

cat <<EOF | sudo dmsetup create tbl
0 1024 linear $DEV 0
EOF
assert_dm tbl 1
sudo dmsetup remove tbl

cat <<EOF | sudo dmsetup create tbl
0 1024 linear $LOOP_DEV 0
1024 2048 error
EOF
assert_dm tbl 0
sudo dmsetup remove tbl

cat <<EOF | sudo dmsetup create tbl
0 1024 linear $DEV 0
1024 2048 error
EOF
assert_dm tbl 1
sudo dmsetup remove tbl

cat <<EOF | sudo dmsetup create tbl
0 1024 linear $LOOP_DEV 0
1024 2048 linear $DEV 0
EOF
assert_dm tbl 1
sudo dmsetup remove tbl

cat <<EOF | sudo dmsetup create tbl
0 1024 linear $DEV 0
1024 2048 linear $LOOP_DEV 0
EOF
assert_dm tbl 1
sudo dmsetup remove tbl

cat <<EOF | sudo dmsetup create tbl
0 1024 linear $LOOP_DEV 0
EOF
assert_dm tbl 0
cat <<EOF | dmsetup_reload tbl
0 1024 linear $LOOP_DEV 0
1024 2048 linear $DEV 0
EOF
assert_dm tbl 1
cat <<EOF | dmsetup_reload tbl
0 1024 linear $LOOP_DEV 0
EOF
assert_dm tbl 0
sudo dmsetup remove tbl

cat <<EOF | sudo dmsetup create tbl
0 1024 linear $DEV 0
EOF
assert_dm tbl 1
cat <<EOF | dmsetup_reload tbl
0 1024 linear $DEV 0
1024 2048 linear $LOOP_DEV 0
EOF
assert_dm tbl 1
cat <<EOF | dmsetup_reload tbl
0 1024 linear $DEV 0
EOF
assert_dm tbl 1
sudo dmsetup remove tbl

cat <<EOF | sudo dmsetup create tbl
0 1024 linear $DEV 0
EOF
assert_dm tbl 1
cat <<EOF | dmsetup_reload tbl
0 1024 linear $DEV 0
1024 2048 linear $LOOP_DEV 0
EOF
assert_dm tbl 1
cat <<EOF | dmsetup_reload tbl
0 1024 error
1024 2048 linear $LOOP_DEV 0
EOF
assert_dm tbl 0
cat <<EOF | dmsetup_reload tbl
0 1024 linear $DEV 0
1024 2048 linear $LOOP_DEV 0
EOF
assert_dm tbl 1
cat <<EOF | dmsetup_reload tbl
0 1024 linear $DEV 0
EOF
assert_dm tbl 1
sudo dmsetup remove tbl

sudo losetup -d $LOOP_DEV
rm loopfile

sudo rbd unmap $DEV
rbd rm $IMAGE_NAME

echo OK
