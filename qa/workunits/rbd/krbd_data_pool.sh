#!/usr/bin/env bash

set -ex

export RBD_FORCE_ALLOW_V1=1

function fill_image() {
    local spec=$1

    local dev
    dev=$(sudo rbd map $spec)
    xfs_io -c "pwrite -b $OBJECT_SIZE -S 0x78 -W 0 $IMAGE_SIZE" $dev
    sudo rbd unmap $dev
}

function create_clones() {
    local spec=$1

    rbd snap create $spec@snap
    rbd snap protect $spec@snap

    local pool=${spec%/*}  # pool/image is assumed
    local image=${spec#*/}
    local child_pool
    for child_pool in $pool clonesonly; do
        rbd clone $spec@snap $child_pool/$pool-$image-clone1
        rbd clone $spec@snap --data-pool repdata $child_pool/$pool-$image-clone2
        rbd clone $spec@snap --data-pool ecdata $child_pool/$pool-$image-clone3
    done
}

function trigger_copyup() {
    local spec=$1

    local dev
    dev=$(sudo rbd map $spec)
    local i
    {
        for ((i = 0; i < $NUM_OBJECTS; i++)); do
            echo pwrite -b $OBJECT_SIZE -S 0x59 $((i * OBJECT_SIZE + OBJECT_SIZE / 2)) $((OBJECT_SIZE / 2))
        done
        echo fsync
        echo quit
    } | xfs_io $dev
    sudo rbd unmap $dev
}

function compare() {
    local spec=$1
    local object=$2

    local dev
    dev=$(sudo rbd map $spec)
    local i
    for ((i = 0; i < $NUM_OBJECTS; i++)); do
        dd if=$dev bs=$OBJECT_SIZE count=1 skip=$i | cmp $object -
    done
    sudo rbd unmap $dev
}

function mkfs_and_mount() {
    local spec=$1

    local dev
    dev=$(sudo rbd map $spec)
    blkdiscard $dev
    mkfs.ext4 -q -E nodiscard $dev
    sudo mount $dev /mnt
    sudo umount /mnt
    sudo rbd unmap $dev
}

function list_HEADs() {
    local pool=$1

    rados -p $pool ls | while read obj; do
        if rados -p $pool stat $obj >/dev/null 2>&1; then
            echo $obj
        fi
    done
}

function count_data_objects() {
    local spec=$1

    local pool
    pool=$(rbd info $spec | grep 'data_pool: ' | awk '{ print $NF }')
    if [[ -z $pool ]]; then
        pool=${spec%/*}  # pool/image is assumed
    fi

    local prefix
    prefix=$(rbd info $spec | grep 'block_name_prefix: ' | awk '{ print $NF }')
    rados -p $pool ls | grep -c $prefix
}

function get_num_clones() {
    local pool=$1

    rados -p $pool --format=json df |
        python -c 'import sys, json; print json.load(sys.stdin)["pools"][0]["num_object_clones"]'
}

ceph osd pool create repdata 24 24
rbd pool init repdata
ceph osd erasure-code-profile set teuthologyprofile crush-failure-domain=osd m=1 k=2
ceph osd pool create ecdata 24 24 erasure teuthologyprofile
rbd pool init ecdata
ceph osd pool set ecdata allow_ec_overwrites true
ceph osd pool create rbdnonzero 24 24
rbd pool init rbdnonzero
ceph osd pool create clonesonly 24 24
rbd pool init clonesonly

for pool in rbd rbdnonzero; do
    rbd create --size 200 --image-format 1 $pool/img0
    rbd create --size 200 $pool/img1
    rbd create --size 200 --data-pool repdata $pool/img2
    rbd create --size 200 --data-pool ecdata $pool/img3
done

IMAGE_SIZE=$(rbd info --format=json img1 | python -c 'import sys, json; print json.load(sys.stdin)["size"]')
OBJECT_SIZE=$(rbd info --format=json img1 | python -c 'import sys, json; print json.load(sys.stdin)["object_size"]')
NUM_OBJECTS=$((IMAGE_SIZE / OBJECT_SIZE))
[[ $((IMAGE_SIZE % OBJECT_SIZE)) -eq 0 ]]

OBJECT_X=$(mktemp)   # xxxx
xfs_io -c "pwrite -b $OBJECT_SIZE -S 0x78 0 $OBJECT_SIZE" $OBJECT_X

OBJECT_XY=$(mktemp)  # xxYY
xfs_io -c "pwrite -b $OBJECT_SIZE -S 0x78 0 $((OBJECT_SIZE / 2))" \
       -c "pwrite -b $OBJECT_SIZE -S 0x59 $((OBJECT_SIZE / 2)) $((OBJECT_SIZE / 2))" \
       $OBJECT_XY

for pool in rbd rbdnonzero; do
    for i in {0..3}; do
        fill_image $pool/img$i
        if [[ $i -ne 0 ]]; then
            create_clones $pool/img$i
            for child_pool in $pool clonesonly; do
                for j in {1..3}; do
                    trigger_copyup $child_pool/$pool-img$i-clone$j
                done
            done
        fi
    done
done

# rbd_directory, rbd_children, rbd_info + img0 header + ...
NUM_META_RBDS=$((3 + 1 + 3 * (1*2 + 3*2)))
# rbd_directory, rbd_children, rbd_info + ...
NUM_META_CLONESONLY=$((3 + 2 * 3 * (3*2)))

[[ $(rados -p rbd ls | wc -l) -eq $((NUM_META_RBDS + 5 * NUM_OBJECTS)) ]]
[[ $(rados -p repdata ls | wc -l) -eq $((1 + 14 * NUM_OBJECTS)) ]]
[[ $(rados -p ecdata ls | wc -l) -eq $((1 + 14 * NUM_OBJECTS)) ]]
[[ $(rados -p rbdnonzero ls | wc -l) -eq $((NUM_META_RBDS + 5 * NUM_OBJECTS)) ]]
[[ $(rados -p clonesonly ls | wc -l) -eq $((NUM_META_CLONESONLY + 6 * NUM_OBJECTS)) ]]

for pool in rbd rbdnonzero; do
    for i in {0..3}; do
        [[ $(count_data_objects $pool/img$i) -eq $NUM_OBJECTS ]]
        if [[ $i -ne 0 ]]; then
            for child_pool in $pool clonesonly; do
                for j in {1..3}; do
                    [[ $(count_data_objects $child_pool/$pool-img$i-clone$j) -eq $NUM_OBJECTS ]]
                done
            done
        fi
    done
done

[[ $(get_num_clones rbd) -eq 0 ]]
[[ $(get_num_clones repdata) -eq 0 ]]
[[ $(get_num_clones ecdata) -eq 0 ]]
[[ $(get_num_clones rbdnonzero) -eq 0 ]]
[[ $(get_num_clones clonesonly) -eq 0 ]]

for pool in rbd rbdnonzero; do
    for i in {0..3}; do
        compare $pool/img$i $OBJECT_X
        mkfs_and_mount $pool/img$i
        if [[ $i -ne 0 ]]; then
            for child_pool in $pool clonesonly; do
                for j in {1..3}; do
                    compare $child_pool/$pool-img$i-clone$j $OBJECT_XY
                done
            done
        fi
    done
done

# mkfs_and_mount should discard some objects everywhere but in clonesonly
[[ $(list_HEADs rbd | wc -l) -lt $((NUM_META_RBDS + 5 * NUM_OBJECTS)) ]]
[[ $(list_HEADs repdata | wc -l) -lt $((1 + 14 * NUM_OBJECTS)) ]]
[[ $(list_HEADs ecdata | wc -l) -lt $((1 + 14 * NUM_OBJECTS)) ]]
[[ $(list_HEADs rbdnonzero | wc -l) -lt $((NUM_META_RBDS + 5 * NUM_OBJECTS)) ]]
[[ $(list_HEADs clonesonly | wc -l) -eq $((NUM_META_CLONESONLY + 6 * NUM_OBJECTS)) ]]

[[ $(get_num_clones rbd) -eq $NUM_OBJECTS ]]
[[ $(get_num_clones repdata) -eq $((2 * NUM_OBJECTS)) ]]
[[ $(get_num_clones ecdata) -eq $((2 * NUM_OBJECTS)) ]]
[[ $(get_num_clones rbdnonzero) -eq $NUM_OBJECTS ]]
[[ $(get_num_clones clonesonly) -eq 0 ]]

echo OK
