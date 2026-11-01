#!/usr/bin/env bash
#
# S3 PUTs and GETs whose data the OSDs move out of band, with elbencho as
# the load generator: many threads, each with a buffer of its own, over
# libfabric (elbencho --s3ofi).
#
# The workunit builds ofi-rma and elbencho on this node, since neither is
# packaged. For each object size, elbencho writes objects with PUTs whose
# payload the OSDs pull, then reads them with GETs whose data the OSDs
# write, and checks every byte (--verify) and the CRC-64/NVME of each
# transfer: the one that the PUT sends, the one that the GET finds
# stored, and the OSDs' CRC of what they moved. elbencho fails on a
# transfer that the gateway declines: there is no HTTP fallback. At the
# end, the OSDs must count a pull for each PUT and a push for each GET.
#
# Environment (see s3-oob-common.sh for RGW_ENDPOINT, OOB_PROVIDER,
# OOB_DOMAIN and OOB_NODE):
#   ELBENCHO_REPO     elbencho with --s3ofi (default:
#                     https://github.com/mmgaggle/elbencho.git)
#   ELBENCHO_REF      its branch, tag or commit (default:
#                     wip-s3rdma-crc64nvme)
#   OFI_RMA_REPO      ofi-rma (default: https://github.com/mmgaggle/ofi-rma.git)
#   OFI_RMA_REF       the ofi-rma that the OSDs run, whose tokens they parse
#                     (default: wip-write-staged)
#   ELBENCHO_THREADS  threads (default 8)
#   ELBENCHO_OBJECTS  objects per thread for each size (default 16)
#   ELBENCHO_SIZES    object sizes (default: 1m 4206316 16m)
#   ELBENCHO_DEPS     0 to skip installing the build dependencies, on a
#                     node that has them (default 1)
#
# Against a vstart cluster with a gateway, from the build directory (see
# run-s3-oob.sh):
#
#   PATH=$PWD/bin:$PATH CEPH_CONF=$PWD/ceph.conf \
#       RGW_ENDPOINT=http://localhost:8000 ELBENCHO_DEPS=0 \
#       ../qa/workunits/rgw/run-s3-oob-elbencho.sh
#
# For verbs, give the gateway by the address of the RDMA NIC, so that the
# client address is on the NIC too.

set -ex

: ${ELBENCHO_REPO:=https://github.com/mmgaggle/elbencho.git}
: ${ELBENCHO_REF:=wip-s3rdma-crc64nvme}
: ${OFI_RMA_REPO:=https://github.com/mmgaggle/ofi-rma.git}
: ${OFI_RMA_REF:=wip-write-staged}
: ${ELBENCHO_THREADS:=8}
: ${ELBENCHO_OBJECTS:=16}
: ${ELBENCHO_SIZES:=1m 4206316 16m}
: ${ELBENCHO_DEPS:=1}

. "$(dirname "$0")/s3-oob-common.sh"

s3oob_user

# under the test directory: a file capability (see below) does nothing
# on a nosuid mount, which /tmp can be
work=$(mktemp -d -p "${TESTDIR:-/tmp}")
trap 'rm -rf "$work"' EXIT

# the build dependencies of elbencho with S3, and of ofi-rma
if [ "$ELBENCHO_DEPS" = 0 ]; then
    :
elif type dnf > /dev/null 2>&1; then
    # libfabric-devel is in CodeReady Builder: crb on CentOS Stream and
    # Rocky, codeready-builder-* on RHEL
    crb=$(dnf repolist --all -q 2>/dev/null |
              awk '$1 == "crb" || $1 ~ /^codeready-builder/ { print $1; exit }')
    sudo dnf install -y ${crb:+--enablerepo=$crb} boost-devel cmake gcc-c++ \
        git libaio-devel libcurl-devel libuuid-devel make numactl-devel \
        openssl-devel zlib-devel libfabric-devel
else
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y \
        libboost-filesystem-dev libboost-program-options-dev \
        libboost-thread-dev cmake g++ git libaio-dev libcurl4-openssl-dev \
        libncurses-dev libnuma-dev libssl-dev uuid-dev zlib1g-dev make \
        libfabric-dev
fi

# a shallow checkout of a branch, a tag or a commit: checkout REPO REF DIR
checkout() {
    git init -q "$3"
    if ! git -C "$3" fetch -q --depth 1 "$1" "$2"; then
        git -C "$3" fetch -q "$1" '+refs/heads/*:refs/remotes/origin/*'
    fi
    git -C "$3" checkout -q FETCH_HEAD 2>/dev/null ||
        git -C "$3" checkout -q "$2"
    git -C "$3" log -1 --format='%H %s'
}

checkout "$OFI_RMA_REPO" "$OFI_RMA_REF" "$work/ofi-rma"
cmake -S "$work/ofi-rma" -B "$work/ofi-rma/build" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX="$work/prefix" \
    -DOFI_RMA_INSTALL=ON -DOFI_RMA_SHARED=OFF -DOFI_RMA_BUILD_TESTS=OFF
cmake --build "$work/ofi-rma/build" -j "$(nproc)"
cmake --install "$work/ofi-rma/build"
PKG_CONFIG_PATH=$(dirname "$(find "$work/prefix" -name ofi-rma.pc | head -n 1)")
export PKG_CONFIG_PATH

checkout "$ELBENCHO_REPO" "$ELBENCHO_REF" "$work/elbencho"
make -C "$work/elbencho" -j "$(nproc)" S3_SUPPORT=1
elbencho=$work/elbencho/bin/elbencho
if ! $elbencho --version | grep -q 'Included.* ofi/s3rdma'; then
    echo "elbencho was built without libfabric (ofi-rma) support"
    exit 1
fi
# the uet provider's software core sends UET frames over a raw socket
case "$OOB_PROVIDER" in
    uet*) sudo setcap cap_net_raw+ep "$elbencho" ;;
esac

args=(--s3endpoints "$RGW_ENDPOINT" --s3key "$access" --s3secret "$secret"
      --s3ofi "$OOB_PROVIDER" --s3chksumalgo CRC64NVME --iodepth 1
      -t "$ELBENCHO_THREADS" -n 1 -N "$ELBENCHO_OBJECTS" --lat)
if [ "$OOB_DOMAIN" != - ]; then
    args+=(--s3ofidomain "$OOB_DOMAIN")
fi
if [ "$OOB_NODE" != - ]; then
    args+=(--s3ofinode "$OOB_NODE")
fi

bucket=s3oob-elbencho
pulls0=$(ofi_sum pulls_completed)
pushes0=$(ofi_sum plans_completed)
$elbencho "${args[@]}" -d "$bucket"
transfers=0
salt=1
for size in $ELBENCHO_SIZES; do
    # PUT, then GET, every object of this size, all single-part
    $elbencho "${args[@]}" -w -r -s "$size" -b "$size" --verify "$salt" \
        "$bucket"
    transfers=$((transfers + ELBENCHO_THREADS * ELBENCHO_OBJECTS))
    salt=$((salt + 1))
done
$elbencho "${args[@]}" -F -D -s 1 -b 1 "$bucket"

# each PUT was pulled and each GET pushed by the OSDs: a GET of an
# erasure-coded object can take a push from each shard, so these count at
# least one per transfer
pulls=$(( $(ofi_sum pulls_completed) - pulls0 ))
pushes=$(( $(ofi_sum plans_completed) - pushes0 ))
echo "transfers $transfers, pulls $pulls, pushes $pushes"
if [ "$pulls" -lt "$transfers" ] || [ "$pushes" -lt "$transfers" ]; then
    echo "the OSDs moved fewer transfers out of band than elbencho made"
    exit 1
fi

echo OK
