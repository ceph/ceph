#!/usr/bin/env bash
set -ex

. $(dirname $0)/../../standalone/ceph-helpers.sh

TEMPDIR=
IMAGE1=image1
IMAGE2=image2
IMAGE3=image3
IMAGES="${IMAGE1} ${IMAGE2} ${IMAGE3}"

cleanup() {
    cleanup_tempdir
    remove_images
}

setup_tempdir() {
    TEMPDIR=`mktemp -d`
}

cleanup_tempdir() {
    rm -rf ${TEMPDIR}
}

create_base_image() {
    local image=$1

    # size is not a multiple of object size to trigger an edge case in
    # list-snaps
    rbd create --size 1025M ${image}

    rbd bench --io-type write --io-pattern rand --io-size=4K --io-total 256M ${image}
    rbd snap create ${image}@1
    rbd bench --io-type write --io-pattern rand --io-size=4K --io-total 64M ${image}
    rbd snap create ${image}@2
    rbd bench --io-type write --io-pattern rand --io-size=4K --io-total 128M ${image}
}

export_raw_image() {
    local image=$1

    rm -rf "${TEMPDIR}/${image}"
    rbd export ${image} "${TEMPDIR}/${image}"
}

export_base_image() {
    local image=$1

    export_raw_image "${image}"
    export_raw_image "${image}@1"
    export_raw_image "${image}@2"
}

remove_image() {
    local image=$1

    (rbd migration abort $image || true) >/dev/null 2>&1
    (rbd snap purge $image || true) >/dev/null 2>&1
    (rbd rm $image || true) >/dev/null 2>&1
}

remove_images() {
    for image in ${IMAGES}
    do
        remove_image ${image}
    done
}

show_diff()
{
    local file1=$1
    local file2=$2

    xxd "${file1}" > "${file1}.xxd"
    xxd "${file2}" > "${file2}.xxd"
    sdiff -s "${file1}.xxd" "${file2}.xxd" | head -n 64
    rm -f "${file1}.xxd" "${file2}.xxd"
}

compare_images() {
    local src_image=$1
    local dst_image=$2
    local ret=0

    export_raw_image ${dst_image}
    if ! cmp "${TEMPDIR}/${src_image}" "${TEMPDIR}/${dst_image}"
    then
        show_diff "${TEMPDIR}/${src_image}" "${TEMPDIR}/${dst_image}"
        ret=1
    fi
    return ${ret}
}

test_import_native_format() {
    local base_image=$1
    local dest_image=$2

    rbd migration prepare --import-only "rbd/${base_image}@2" ${dest_image}
    rbd migration abort ${dest_image}

    local pool_id=$(ceph osd pool ls detail --format xml | xmlstarlet sel -t -v "//pools/pool[pool_name='rbd']/pool_id")
    cat > ${TEMPDIR}/spec.json <<EOF
{
  "type": "native",
  "pool_id": ${pool_id},
  "pool_namespace": "",
  "image_name": "${base_image}",
  "snap_name": "2"
}
EOF
    cat ${TEMPDIR}/spec.json

    rbd migration prepare --import-only \
	--source-spec-path ${TEMPDIR}/spec.json ${dest_image}

    compare_images "${base_image}@1" "${dest_image}@1"
    compare_images "${base_image}@2" "${dest_image}@2"

    rbd migration abort ${dest_image}

    rbd migration prepare --import-only \
        --source-spec-path ${TEMPDIR}/spec.json ${dest_image}
    rbd migration execute ${dest_image}

    compare_images "${base_image}@1" "${dest_image}@1"
    compare_images "${base_image}@2" "${dest_image}@2"

    rbd migration abort ${dest_image}

    rbd migration prepare --import-only \
        --source-spec "{\"type\": \"native\", \"pool_id\": "${pool_id}", \"image_name\": \"${base_image}\", \"snap_name\": \"2\"}" \
        ${dest_image}
    rbd migration abort ${dest_image}

    rbd migration prepare --import-only \
        --source-spec "{\"type\": \"native\", \"pool_name\": \"rbd\", \"image_name\": \"${base_image}\", \"snap_name\": \"2\"}" \
        ${dest_image}
    rbd migration execute ${dest_image}
    rbd migration commit ${dest_image}

    compare_images "${base_image}@1" "${dest_image}@1"
    compare_images "${base_image}@2" "${dest_image}@2"

    remove_image "${dest_image}"
}

test_import_qcow_format() {
    local base_image=$1
    local dest_image=$2

    if ! qemu-img convert -f raw -O qcow rbd:rbd/${base_image} ${TEMPDIR}/${base_image}.qcow; then
        echo "skipping QCOW test"
        return 0
    fi
    qemu-img info -f qcow ${TEMPDIR}/${base_image}.qcow

    cat > ${TEMPDIR}/spec.json <<EOF
{
  "type": "qcow",
  "stream": {
    "type": "file",
    "file_path": "${TEMPDIR}/${base_image}.qcow"
  }
}
EOF
    cat ${TEMPDIR}/spec.json

    set +e
    rbd migration prepare --import-only \
        --source-spec-path ${TEMPDIR}/spec.json ${dest_image}
    local error_code=$?
    set -e

    if [ $error_code -eq 95 ]; then
        echo "skipping QCOW test (librbd support disabled)"
        return 0
    fi
    test $error_code -eq 0

    compare_images "${base_image}" "${dest_image}"

    rbd migration abort ${dest_image}

    rbd migration prepare --import-only \
        --source-spec-path ${TEMPDIR}/spec.json ${dest_image}

    compare_images "${base_image}" "${dest_image}"

    rbd migration execute ${dest_image}

    compare_images "${base_image}" "${dest_image}"

    rbd migration commit ${dest_image}

    compare_images "${base_image}" "${dest_image}"

    remove_image "${dest_image}"
}

test_import_qcow2_format() {
    local base_image=$1
    local dest_image=$2

    # create new image via qemu-img and its bench tool since we cannot
    # import snapshot deltas into QCOW2
    qemu-img create -f qcow2 ${TEMPDIR}/${base_image}.qcow2 1G

    qemu-img bench -f qcow2 -w -c 65536 -d 16 --pattern 65 -s 4096 \
        -S $((($RANDOM % 262144) * 4096)) ${TEMPDIR}/${base_image}.qcow2
    qemu-img convert -f qcow2 -O raw ${TEMPDIR}/${base_image}.qcow2 \
        "${TEMPDIR}/${base_image}@snap1"
    qemu-img snapshot -c "snap1" ${TEMPDIR}/${base_image}.qcow2

    qemu-img bench -f qcow2 -w -c 16384 -d 16 --pattern 66 -s 4096 \
        -S $((($RANDOM % 262144) * 4096)) ${TEMPDIR}/${base_image}.qcow2
    qemu-img convert -f qcow2 -O raw ${TEMPDIR}/${base_image}.qcow2 \
        "${TEMPDIR}/${base_image}@snap2"
    qemu-img snapshot -c "snap2" ${TEMPDIR}/${base_image}.qcow2

    qemu-img bench -f qcow2 -w -c 32768 -d 16 --pattern 67 -s 4096 \
        -S $((($RANDOM % 262144) * 4096)) ${TEMPDIR}/${base_image}.qcow2
    qemu-img convert -f qcow2 -O raw ${TEMPDIR}/${base_image}.qcow2 \
        ${TEMPDIR}/${base_image}

    qemu-img info -f qcow2 ${TEMPDIR}/${base_image}.qcow2

    cat > ${TEMPDIR}/spec.json <<EOF
{
  "type": "qcow",
  "stream": {
    "type": "file",
    "file_path": "${TEMPDIR}/${base_image}.qcow2"
  }
}
EOF
    cat ${TEMPDIR}/spec.json

    rbd migration prepare --import-only \
        --source-spec-path ${TEMPDIR}/spec.json ${dest_image}

    compare_images "${base_image}@snap1" "${dest_image}@snap1"
    compare_images "${base_image}@snap2" "${dest_image}@snap2"
    compare_images "${base_image}" "${dest_image}"

    rbd migration abort ${dest_image}

    rbd migration prepare --import-only \
        --source-spec-path ${TEMPDIR}/spec.json ${dest_image}

    compare_images "${base_image}@snap1" "${dest_image}@snap1"
    compare_images "${base_image}@snap2" "${dest_image}@snap2"
    compare_images "${base_image}" "${dest_image}"

    rbd migration execute ${dest_image}

    compare_images "${base_image}@snap1" "${dest_image}@snap1"
    compare_images "${base_image}@snap2" "${dest_image}@snap2"
    compare_images "${base_image}" "${dest_image}"

    rbd migration commit ${dest_image}

    compare_images "${base_image}@snap1" "${dest_image}@snap1"
    compare_images "${base_image}@snap2" "${dest_image}@snap2"
    compare_images "${base_image}" "${dest_image}"

    remove_image "${dest_image}"
}

test_import_raw_format() {
    local base_image=$1
    local dest_image=$2

    cat > ${TEMPDIR}/spec.json <<EOF
{
  "type": "raw",
  "stream": {
    "type": "file",
    "file_path": "${TEMPDIR}/${base_image}"
  }
}
EOF
    cat ${TEMPDIR}/spec.json

    cat ${TEMPDIR}/spec.json | rbd migration prepare --import-only \
	--source-spec-path - ${dest_image}
    compare_images ${base_image} ${dest_image}
    rbd migration abort ${dest_image}

    rbd migration prepare --import-only \
	--source-spec-path ${TEMPDIR}/spec.json ${dest_image}
    rbd migration execute ${dest_image}
    rbd migration commit ${dest_image}

    compare_images ${base_image} ${dest_image}

    remove_image "${dest_image}"

    cat > ${TEMPDIR}/spec.json <<EOF
{
  "type": "raw",
  "stream": {
    "type": "file",
    "file_path": "${TEMPDIR}/${base_image}"
  },
  "snapshots": [{
    "type": "raw",
    "name": "snap1",
    "stream": {
      "type": "file",
      "file_path": "${TEMPDIR}/${base_image}@1"
     }
  }, {
    "type": "raw",
    "name": "snap2",
    "stream": {
      "type": "file",
      "file_path": "${TEMPDIR}/${base_image}@2"
     }
  }]
}
EOF
    cat ${TEMPDIR}/spec.json

    rbd migration prepare --import-only \
        --source-spec-path ${TEMPDIR}/spec.json ${dest_image}

    rbd snap create ${dest_image}@head
    rbd bench --io-type write --io-pattern rand --io-size=32K --io-total=32M ${dest_image}

    compare_images "${base_image}" "${dest_image}@head"
    compare_images "${base_image}@1" "${dest_image}@snap1"
    compare_images "${base_image}@2" "${dest_image}@snap2"
    compare_images "${base_image}" "${dest_image}@head"

    rbd migration execute ${dest_image}

    compare_images "${base_image}@1" "${dest_image}@snap1"
    compare_images "${base_image}@2" "${dest_image}@snap2"
    compare_images "${base_image}" "${dest_image}@head"

    rbd migration commit ${dest_image}

    remove_image "${dest_image}"
}

# make sure rbd pool is EMPTY.. this is a test script!!
rbd ls 2>&1 | wc -l | grep -v '^0$' && echo "nonempty rbd pool, aborting!  run this script on an empty test cluster only." && exit 1

setup_tempdir
trap 'cleanup $?' INT TERM EXIT

create_base_image ${IMAGE1}
export_base_image ${IMAGE1}

test_import_native_format ${IMAGE1} ${IMAGE2}
test_import_qcow_format ${IMAGE1} ${IMAGE2}
test_import_qcow2_format ${IMAGE2} ${IMAGE3}
test_import_raw_format ${IMAGE1} ${IMAGE2}

echo OK
