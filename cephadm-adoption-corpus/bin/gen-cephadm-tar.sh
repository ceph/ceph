#!/bin/bash -ex

#
# Generates a tarball for use with the `cephadm` tests
#

DIRS=(
   "/etc/ceph"
   "/var/lib/ceph"
   "/var/log/ceph"
)

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})

TMPDIR=$(mktemp -d ${SCRIPT_NAME%.*}.XXX)
trap "rm -rf $TMPDIR" EXIT

TARBALL="$1"
if [ -z "$TARBALL" ]; then
    echo "tarball not specified" 1>&2
    exit 1
fi

# `systemctl stop` all services
systemctl list-units | grep ceph
systemctl stop ceph.target

# Copy the required dirs
for dir in "${DIRS[@]}"; do
    cp -a --parents --sparse=always $dir $TMPDIR
done

# Convert block dev(s) into a sparse file
SYMLINKS=$(find $TMPDIR -type l)
for symlink in $SYMLINKS; do
    file -L $symlink |  grep "block special" || exit 1

    # convert the block dev into a sparse file
    blockdev=${TMPDIR}/$(realpath $symlink)
    mkdir -p $(dirname $blockdev)
    dd if=$symlink of=$blockdev conv=sparse

    # symlink to the sparse file
    symlink_tgt=$(realpath --relative-to=$(dirname $symlink) $blockdev)
    rm -f $symlink && ln -s $symlink_tgt $symlink
done

# Output the tarball
tar czvSpf $TARBALL -C $TMPDIR .
