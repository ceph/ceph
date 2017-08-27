#!/usr/bin/env bash

set -ex

function expect_false() {
    if "$@"; then return 1; else return 0; fi
}

function assert_locked() {
    local dev_id="${1#/dev/rbd}"

    local client_addr
    client_addr="$(< $SYSFS_DIR/$dev_id/client_addr)"

    local client_id
    client_id="$(< $SYSFS_DIR/$dev_id/client_id)"
    # client4324 -> client.4324
    client_id="client.${client_id#client}"

    local watch_cookie
    watch_cookie="$(rados -p rbd listwatchers rbd_header.$IMAGE_ID |
        grep $client_id | cut -d ' ' -f 3 | cut -d '=' -f 2)"
    [[ $(echo -n "$watch_cookie" | grep -c '^') -eq 1 ]]

    local actual
    actual="$(rados -p rbd --format=json lock info rbd_header.$IMAGE_ID rbd_lock |
        python -m json.tool)"

    local expected
    expected="$(cat <<EOF | python -m json.tool
{
    "lockers": [
        {
            "addr": "$client_addr",
            "cookie": "auto $watch_cookie",
            "description": "",
            "expiration": "0.000000",
            "name": "$client_id"
        }
    ],
    "name": "rbd_lock",
    "tag": "internal",
    "type": "exclusive"
}
EOF
    )"

    [ "$actual" = "$expected" ]
}

function assert_unlocked() {
    rados -p rbd --format=json lock info rbd_header.$IMAGE_ID rbd_lock |
        grep '"lockers":\[\]'
}

SYSFS_DIR="/sys/bus/rbd/devices"
IMAGE_NAME="exclusive-option-test"

rbd create --size 1 --image-feature '' $IMAGE_NAME

IMAGE_ID="$(rbd info --format=json $IMAGE_NAME |
    python -c "import sys, json; print json.load(sys.stdin)['block_name_prefix'].split('.')[1]")"

DEV=$(sudo rbd map $IMAGE_NAME)
assert_unlocked
sudo rbd unmap $DEV
assert_unlocked

expect_false sudo rbd map -o exclusive $IMAGE_NAME
assert_unlocked

rbd feature enable $IMAGE_NAME exclusive-lock
rbd snap create $IMAGE_NAME@snap

DEV=$(sudo rbd map $IMAGE_NAME)
assert_unlocked
sudo rbd unmap $DEV
assert_unlocked

DEV=$(sudo rbd map -o exclusive $IMAGE_NAME)
assert_locked $DEV
[[ $(blockdev --getro $DEV) -eq 0 ]]
sudo rbd unmap $DEV
assert_unlocked

DEV=$(sudo rbd map -o exclusive $IMAGE_NAME@snap)
assert_locked $DEV
[[ $(blockdev --getro $DEV) -eq 1 ]]
sudo rbd unmap $DEV
assert_unlocked

DEV=$(sudo rbd map -o exclusive,ro $IMAGE_NAME)
assert_locked $DEV
[[ $(blockdev --getro $DEV) -eq 1 ]]
sudo rbd unmap $DEV
assert_unlocked

# alternate syntax
DEV=$(sudo rbd map --exclusive --read-only $IMAGE_NAME)
assert_locked $DEV
[[ $(blockdev --getro $DEV) -eq 1 ]]
sudo rbd unmap $DEV
assert_unlocked

DEV=$(sudo rbd map $IMAGE_NAME)
assert_unlocked
dd if=/dev/urandom of=$DEV bs=4k count=10 oflag=direct
assert_locked $DEV
OTHER_DEV=$(sudo rbd map -o noshare,exclusive $IMAGE_NAME)
assert_locked $OTHER_DEV
sudo rbd unmap $DEV
sudo rbd unmap $OTHER_DEV
assert_unlocked

DEV=$(sudo rbd map -o exclusive $IMAGE_NAME)
assert_locked $DEV
expect_false sudo rbd map -o noshare,exclusive $IMAGE_NAME
assert_locked $DEV
sudo rbd unmap $DEV
assert_unlocked

DEV=$(sudo rbd map -o exclusive $IMAGE_NAME)
assert_locked $DEV
OTHER_DEV=$(sudo rbd map -o noshare $IMAGE_NAME)
dd if=/dev/urandom of=$OTHER_DEV bs=4k count=10 oflag=direct &
PID=$!
sleep 20
assert_locked $DEV
[ "$(ps -o stat= $PID)" = "D" ]
sudo rbd unmap $DEV
wait $PID
assert_locked $OTHER_DEV
sudo rbd unmap $OTHER_DEV
assert_unlocked

DEV=$(sudo rbd map -o exclusive $IMAGE_NAME)
assert_locked $DEV
sudo rbd map -o noshare,lock_on_read $IMAGE_NAME &
SUDO_PID=$!
sleep 20
assert_locked $DEV
PID="$(ps -o pid= --ppid $SUDO_PID)"
[ "$(ps -o stat= $PID)" = "Dl" ]
sudo rbd unmap $DEV
wait $SUDO_PID
assert_locked $OTHER_DEV
sudo rbd unmap $OTHER_DEV
assert_unlocked

# induce a watch error after 30 seconds
DEV=$(sudo rbd map -o exclusive,osdkeepalive=60 $IMAGE_NAME)
assert_locked $DEV
OLD_WATCHER="$(rados -p rbd listwatchers rbd_header.$IMAGE_ID)"
sleep 40
assert_locked $DEV
NEW_WATCHER="$(rados -p rbd listwatchers rbd_header.$IMAGE_ID)"
# same client_id, old cookie < new cookie
[ "$(echo "$OLD_WATCHER" | cut -d ' ' -f 2)" = \
    "$(echo "$NEW_WATCHER" | cut -d ' ' -f 2)" ]
[[ $(echo "$OLD_WATCHER" | cut -d ' ' -f 3 | cut -d '=' -f 2) -lt \
    $(echo "$NEW_WATCHER" | cut -d ' ' -f 3 | cut -d '=' -f 2) ]]
sudo rbd unmap $DEV
assert_unlocked

echo OK
