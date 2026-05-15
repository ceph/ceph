#!/bin/bash
set -euo pipefail

FS_NAME="${1:-cephfs}"
SUBVOL_GROUP="${2:-mirror-group}"
SUBVOL_NAME="${3:-mirror-subvol}"
MOUNT_POINT="${4:-/mnt/cephfs}"

echo "===== Gathering Cluster Information ====="

MON_HOSTS=$(ceph mon dump -f json | jq -r '.mons[].public_addrs.addrvec[0].addr' | cut -d':' -f1 | paste -sd "," -)

echo "Monitors: ${MON_HOSTS}"

echo
echo "===== Creating CephFS Client ====="

if ! ceph auth get client.mirror-test >/dev/null 2>&1; then
    ceph fs authorize "${FS_NAME}" client.mirror-test / rw
fi

SECRET=$(ceph auth get-key client.mirror-test)

mkdir -p /etc/ceph

cat >/etc/ceph/ceph.client.mirror-test.keyring <<EOF
[client.mirror-test]
    key = ${SECRET}
EOF

echo
echo "===== Mounting CephFS ====="

mkdir -p "${MOUNT_POINT}"

if mount | grep -q "${MOUNT_POINT}"; then
    echo "CephFS already mounted"
else
    mount -t ceph \
      "${MON_HOSTS}:/" \
      "${MOUNT_POINT}" \
      -o name=mirror-test,secret="${SECRET}"
fi

echo
echo "===== Creating Subvolume Group ====="

if ceph fs subvolumegroup info "${FS_NAME}" "${SUBVOL_GROUP}" >/dev/null 2>&1; then
    echo "Subvolume group exists"
else
    ceph fs subvolumegroup create \
        "${FS_NAME}" \
        "${SUBVOL_GROUP}"
fi

echo
echo "===== Creating Subvolume ====="

if ceph fs subvolume info \
    "${FS_NAME}" \
    "${SUBVOL_NAME}" \
    --group_name "${SUBVOL_GROUP}" >/dev/null 2>&1; then
    echo "Subvolume exists"
else
    ceph fs subvolume create \
        "${FS_NAME}" \
        "${SUBVOL_NAME}" \
        --group_name "${SUBVOL_GROUP}" \
        --size 1073741824
fi

echo
echo "===== Getting Subvolume Path ====="

SUBVOL_PATH=$(ceph fs subvolume getpath \
    "${FS_NAME}" \
    "${SUBVOL_NAME}" \
    --group_name "${SUBVOL_GROUP}")

echo "Subvolume Path: ${SUBVOL_PATH}"

echo
echo "===== Adding Path To Mirroring ====="

ADD_OUTPUT=$(
    ceph fs snapshot mirror add \
        "${FS_NAME}" \
        "${SUBVOL_PATH}" 2>&1
) || true

if echo "${ADD_OUTPUT}" | grep -q "EEXIST\|already tracked"; then
    echo "Path already configured for mirroring"
else
    echo "Added path to mirroring"
fi

echo
echo "===== Writing Test Data ====="

mkdir -p "${MOUNT_POINT}${SUBVOL_PATH}"

for i in {1..10}; do
    dd if=/dev/urandom \
       of="${MOUNT_POINT}${SUBVOL_PATH}/file-${i}.bin" \
       bs=1M \
       count=10 \
       status=none

    echo "Created file-${i}.bin"
done

echo
echo "===== Creating Snapshot ====="

SNAP_NAME="snap-$(date +%Y%m%d-%H%M%S)"

ceph fs subvolume snapshot create \
    "${FS_NAME}" \
    "${SUBVOL_NAME}" \
    "${SNAP_NAME}" \
    --group_name "${SUBVOL_GROUP}"

echo "Snapshot Created: ${SNAP_NAME}"

echo
echo "===== Mirroring Status ====="

ceph fs snapshot mirror ls "${FS_NAME}"

echo
ceph fs snapshot mirror status "${FS_NAME}"

echo
echo "===== Complete ====="
echo "Subvolume Group : ${SUBVOL_GROUP}"
echo "Subvolume       : ${SUBVOL_NAME}"
echo "Path            : ${SUBVOL_PATH}"
echo "Snapshot        : ${SNAP_NAME}"

