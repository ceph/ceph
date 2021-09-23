#!/bin/bash

set -ex

IMAGE=quay.ceph.io/ceph-ci/ceph:master
docker pull $IMAGE
# update image with deps
docker build -t $IMAGE docker/ceph
# store to later load within docker
mkdir -p docker/ceph/image
rm -f docker/ceph/image/quay.ceph.image.tar
docker save quay.ceph.io/ceph-ci/ceph:master -o docker/ceph/image/quay.ceph.image.tar
