#!/bin/bash

set -x
set -e
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

# Run me from your build dir!  I look for binaries in bin/, lib/ etc.
BUILDPATH=$(pwd)

# PREREQUISITE: a built rook image to use as a base, either self built
# or from dockerhub.  If you ran "make" in your rook source checkout
# you'll have one like build-<hash>/rook-amd64
DEFAULT_BASEIMAGE="`docker image ls | grep ceph-amd64 | cut -d " " -f 1`"
BASEIMAGE="${BASEIMAGE:-$DEFAULT_BASEIMAGE}"

# PREREQUISITE: a repo that you can push to.  You are probably running
# a local docker registry that your kubelet nodes also have access to.
if [ -z "$REPO" ]
then
    echo "ERROR: no \$REPO set!"
    echo "Run a docker repository and set REPO to <hostname>:<port>"
    exit -1
fi

# The output image name: this should match whatever is configured as
# the image name in your Rook cluster CRD object.
IMAGE=rook/ceph
TAG=$(git rev-parse --short HEAD)

# The namespace where ceph containers are running in your
# test cluster: used for bouncing the containers.
NAMESPACE=rook-ceph

mkdir -p kubejacker
cp $SCRIPTPATH/Dockerfile kubejacker
sed -i s@BASEIMAGE@$BASEIMAGE@ kubejacker/Dockerfile

# TODO: let user specify which daemon they're interested
# in -- doing all bins all the time is too slow and bloaty
BINS="ceph-mgr ceph-mon ceph-mds ceph-osd rados radosgw-admin radosgw"
pushd bin
strip $BINS  #TODO: make stripping optional
tar czf $BUILDPATH/kubejacker/bin.tar.gz $BINS
popd

# We need ceph-common to support the binaries
# We need librados/rbd to support mgr modules
# that import the python bindings
LIBS="libceph-common.so.0 libceph-common.so librados.so.2 librados.so librados.so.2.0.0 librbd.so librbd.so.1 librbd.so.1.12.0"
pushd lib
strip $LIBS  #TODO: make stripping optional
tar czf $BUILDPATH/kubejacker/lib.tar.gz $LIBS
popd

pushd ../src/pybind/mgr
find ./ -name "*.pyc" -exec rm -f {} \;
# Exclude node_modules because it's the huge sources in dashboard/frontend
tar --exclude=node_modules --exclude=tests --exclude-backups -czf $BUILDPATH/kubejacker/mgr_plugins.tar.gz *
popd

ECLIBS="libec_*.so"
pushd lib
strip $ECLIBS  #TODO: make stripping optional
tar czf $BUILDPATH/kubejacker/eclib.tar.gz $ECLIBS
popd


pushd kubejacker
docker build -t $REPO/$IMAGE:$TAG .
popd

# Push the image to the repository
docker tag $REPO/$IMAGE:$TAG $REPO/$IMAGE:latest
docker push $REPO/$IMAGE:latest
docker push $REPO/$IMAGE:$TAG 

# Finally, bounce the containers to pick up the new image
kubectl -n $NAMESPACE delete pod -l app=rook-ceph-mds
kubectl -n $NAMESPACE delete pod -l app=rook-ceph-mgr
kubectl -n $NAMESPACE delete pod -l app=rook-ceph-mon
