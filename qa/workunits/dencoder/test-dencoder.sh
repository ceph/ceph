#!/usr/bin/env bash
set -ex
CEPH_ARGS=""
mydir=`dirname $0`
readable=$(realpath $mydir/../../../src/test/encoding/readable.sh)

# clone the corpus repository on the host
git clone -b master https://github.com/ceph/ceph-object-corpus.git $CEPH_MNT/client.0/tmp/ceph-object-corpus-master

export CORPUS_PATH=$CEPH_MNT/client.0/tmp/ceph-object-corpus-master

# With DENCODER_IMAGE set, use the ceph-dencoder from that container image
# instead of the host packages, so older releases don't need packages built
# for the host distro.
if [ -n "$DENCODER_IMAGE" ]; then
    sudo podman run --rm --entrypoint bash \
        -e CORPUS_PATH="$CORPUS_PATH" \
        -v "$CORPUS_PATH:$CORPUS_PATH:ro,z" \
        -v "$readable:/readable.sh:ro,z" \
        "$DENCODER_IMAGE" -c 'ceph-dencoder version && /readable.sh'
else
    ceph-dencoder version
    $readable
fi

echo $0 OK
