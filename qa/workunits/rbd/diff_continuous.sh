#!/usr/bin/env bash

set -ex
set -o pipefail

function untar_workload() {
    local i
    for ((i = 0; i < 10; i++)); do
        pv -L 10M linux-5.4.tar.gz > "${MOUNT}/linux-5.4.tar.gz"
        tar -C "${MOUNT}" -xzf "${MOUNT}/linux-5.4.tar.gz"
        sync "${MOUNT}"
        rm -rf "${MOUNT}"/linux-5.4*
    done
}

function check_object_map() {
    local spec="$1"

    rbd object-map check "${spec}"

    local flags
    flags="$(rbd info "${spec}" | grep 'flags: ')"
    if [[ "${flags}" =~ object\ map\ invalid ]]; then
        echo "Object map invalid at ${spec}"
        exit 1
    fi
    if [[ "${flags}" =~ fast\ diff\ invalid ]]; then
        echo "Fast diff invalid at ${spec}"
        exit 1
    fi
}

# RBD_DEVICE_TYPE is intended to be set from yaml, default to krbd
readonly DEVICE_TYPE="${RBD_DEVICE_TYPE:-krbd}"

BASE_UUID="$(uuidgen)"
readonly BASE_UUID

readonly SIZE="2G"
readonly SRC="${BASE_UUID}-src"
readonly DST="${BASE_UUID}-dst"
readonly MOUNT="${BASE_UUID}-mnt"

rbd create -s "${SIZE}" --stripe-unit 64K --stripe-count 8 \
    --image-feature exclusive-lock,object-map,fast-diff "${SRC}"
rbd create -s "${SIZE}" --object-size 512K "${DST}"

dev="$(sudo rbd device map -t "${DEVICE_TYPE}" "${SRC}")"
sudo mkfs.ext4 "${dev}"
mkdir "${MOUNT}"
sudo mount "${dev}" "${MOUNT}"
sudo chown "$(whoami)" "${MOUNT}"

# start untar in the background
wget https://download.ceph.com/qa/linux-5.4.tar.gz
untar_workload &
untar_pid=$!

# export initial incremental
snap_num=1
rbd snap create "${SRC}@snap${snap_num}"
rbd export-diff "${SRC}@snap${snap_num}" "${BASE_UUID}@snap${snap_num}.diff"

# keep exporting successive incrementals while untar is running
while kill -0 "${untar_pid}"; do
    snap_num=$((snap_num + 1))
    rbd snap create "${SRC}@snap${snap_num}"
    sleep $((RANDOM % 4 + 1))
    rbd export-diff --whole-object --from-snap "snap$((snap_num - 1))" \
        "${SRC}@snap${snap_num}" "${BASE_UUID}@snap${snap_num}.diff"
done

sudo umount "${MOUNT}"
sudo rbd device unmap -t "${DEVICE_TYPE}" "${dev}"

if ! wait "${untar_pid}"; then
    echo "untar_workload failed"
    exit 1
fi

echo "Exported ${snap_num} incrementals"
if ((snap_num < 30)); then
    echo "Too few incrementals"
    exit 1
fi

# validate
for ((i = 1; i <= snap_num; i++)); do
    rbd import-diff "${BASE_UUID}@snap${i}.diff" "${DST}"
    src_sum="$(rbd export "${SRC}@snap${i}" - | md5sum | awk '{print $1}')"
    dst_sum="$(rbd export "${DST}@snap${i}" - | md5sum | awk '{print $1}')"
    if [[ "${src_sum}" != "${dst_sum}" ]]; then
        echo "Mismatch at snap${i}: ${src_sum} != ${dst_sum}"
        exit 1
    fi
    check_object_map "${SRC}@snap${i}"
    # FIXME: this reproduces http://tracker.ceph.com/issues/37876
    # there is no fstrim involved but "rbd import-diff" can produce
    # write-zeroes requests which turn into discards under the hood
    # actual: EXISTS, expected: EXISTS_CLEAN inconsistency is harmless
    # from a data integrity POV and data is validated above regardless,
    # so just waive it for now
    #check_object_map "${DST}@snap${i}"
done

echo OK
