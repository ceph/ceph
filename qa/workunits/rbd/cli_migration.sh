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

    rbd create --size 1G ${image}
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

compare_images() {
    local src_image=$1
    local dst_image=$2

    export_raw_image ${dst_image}
    cmp "${TEMPDIR}/${src_image}" "${TEMPDIR}/${dst_image}"
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
test_import_raw_format ${IMAGE1} ${IMAGE2}

echo OK
