#!/usr/bin/env bash
#
# S3 GETs and PUTs whose data the OSDs move out of band, over libfabric.
#
# ceph_test_rgw_ofi_put sends a PUT whose payload the OSDs pull from the
# client's memory, and ceph_test_rgw_ofi_get a GET whose data the OSDs
# write into it. Each checks every byte and the OSDs' CRC. The objects
# cover sizes around the stripe and chunk boundaries, ranged GETs,
# CRC-32C, and objects that a plain HTTP PUT wrote. The clients move no
# empty objects; s3tests covers those over HTTP.
#
# Environment (see s3-oob-common.sh for RGW_ENDPOINT, OOB_PROVIDER,
# OOB_DOMAIN and OOB_NODE):
#   OOB_MODE      strict: every transfer goes out of band, and the OSDs
#                 must report pushed and pulled bytes (default)
#                 mixed: a fallback to HTTP is allowed, but every byte
#                 must still match, as while daemons of two versions run
#   OOB_BUCKETS   buckets to use, one per pool layout (default s3oob)
#   OOB_MAX_SIZE  the largest object (default 64 MiB)
#   OOB_OPS       the transfers that go out of band: "get", "put" or both
#                 (default "get put"); the others go over plain HTTP
#
# Against a vstart cluster with a gateway, from the build directory:
#
#   ../src/vstart.sh -n -x --rgw_port 8000 \
#       -o 'oob_client_transports = tcp' -o 'oob_network = 127.0.0.0/8' \
#       -o 'oob_memory = 268435456'
#   PATH=$PWD/bin:$PATH CEPH_CONF=$PWD/ceph.conf \
#       RGW_ENDPOINT=http://localhost:8000 ../qa/workunits/rgw/run-s3-oob.sh

set -ex

: ${OOB_MODE:=strict}
: ${OOB_BUCKETS:=s3oob}
: ${OOB_MAX_SIZE:=67108864}
: ${OOB_OPS:=get put}

. "$(dirname "$0")/s3-oob-common.sh"

for tool in ceph_test_rgw_ofi_get ceph_test_rgw_ofi_put; do
    if ! type $tool > /dev/null 2>&1; then
        echo "$tool not found: the build has no WITH_OOB_OFI"
        exit 1
    fi
done

has_op() {
    case " $OOB_OPS " in
        *" $1 "*) return 0 ;;
    esac
    return 1
}
if ! has_op get && ! has_op put; then
    echo "OOB_OPS names get, put or both, not $OOB_OPS"
    exit 1
fi

case "$OOB_MODE" in
    strict) export OFI_S3_ALLOW_FALLBACK=0 ;;
    mixed) export OFI_S3_ALLOW_FALLBACK=1 ;;
    *) echo "OOB_MODE is strict or mixed, not $OOB_MODE"; exit 1 ;;
esac

s3oob_user

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

get() {
    ceph_test_rgw_ofi_get "$OOB_PROVIDER" "$OOB_DOMAIN" "$OOB_NODE" \
        "$RGW_ENDPOINT" "$1" "$2" $access $secret "$3"
}

put() {
    ceph_test_rgw_ofi_put "$OOB_PROVIDER" "$OOB_DOMAIN" "$OOB_NODE" \
        "$RGW_ENDPOINT" "$1" "$2" $access $secret "$3"
}

pushed0=$(ofi_sum bytes_pushed)
pulled0=$(ofi_sum bytes_pulled)
failed0=$(ofi_sum plans_failed)
cancels0=$(ofi_sum cancels_failed)

# sizes around the boundaries of a 4 KiB page, a 64 KiB chunk and a
# 4 MiB rados object, plus a large object
sizes="1 4095 4096 65537 1048583 4194304 4194305 $OOB_MAX_SIZE"

for bucket in $OOB_BUCKETS; do
    s3 PUT "$bucket" > /dev/null || true

    for size in $sizes; do
        f=$work/obj-$size
        head -c $size /dev/urandom > "$f"

        if has_op put; then
            put "$bucket" "oob-$size" "$f"
            OFI_S3_CKSUM=CRC32C put "$bucket" "c32-$size" "$f"
            # a plain GET of an object the OSDs pulled
            s3 GET "$bucket/oob-$size" > "$work/back"
            cmp "$f" "$work/back"
        else
            s3 PUT "$bucket/oob-$size" "$f" > /dev/null
            s3 PUT "$bucket/c32-$size" "$f" > /dev/null
        fi

        if has_op get; then
            get "$bucket" "oob-$size" "$f"

            # an object a plain PUT wrote, read out of band
            s3 PUT "$bucket/http-$size" "$f" > /dev/null
            get "$bucket" "http-$size" "$f"

            if [ $size -gt 2 ]; then
                # a range in the middle, and the last byte
                OFI_S3_RANGE=1-$((size - 2)) get "$bucket" "oob-$size" "$f"
                OFI_S3_RANGE=$((size - 1))-$((size - 1)) \
                    get "$bucket" "oob-$size" "$f"
            fi
            OFI_S3_CKSUM=CRC32C get "$bucket" "c32-$size" "$f"
        fi
    done
done

pushed=$(( $(ofi_sum bytes_pushed) - pushed0 ))
pulled=$(( $(ofi_sum bytes_pulled) - pulled0 ))
failed=$(( $(ofi_sum plans_failed) - failed0 ))
cancels=$(( $(ofi_sum cancels_failed) - cancels0 ))
echo "OSDs pushed $pushed and pulled $pulled bytes, $failed plans failed," \
     "$cancels cancels failed"

if [ $cancels -ne 0 ]; then
    echo "a cut-off could not cancel a write"
    exit 1
fi
if [ "$OOB_MODE" = strict ]; then
    if has_op get && [ $pushed -le 0 ]; then
        echo "the OSDs pushed no GET data out of band"
        exit 1
    fi
    if has_op put && [ $pulled -le 0 ]; then
        echo "the OSDs pulled no PUT data out of band"
        exit 1
    fi
    if [ $failed -ne 0 ]; then
        echo "transfers failed"
        exit 1
    fi
fi

echo OK
