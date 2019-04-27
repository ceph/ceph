#!/bin/bash

set -x
set -e
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

# Run me from your build dir!  I look for binaries in bin/, lib/ etc.
BUILDPATH=$(pwd)


# PREREQUISITE: a repo that you can push to.  You are probably running
# a local docker registry that your kubelet nodes also have access to.
REPO=${REPO:-"$1"}

if [ -z "$REPO" ]
then
    echo "ERROR: no \$REPO set!"
    echo "Run a docker repository and set REPO to <hostname>:<port>"
    exit -1
fi

# The output image name: this should match whatever is configured as
# the image name in your Rook cluster CRD object.
IMAGE=ceph/ceph
TAG=latest

# The namespace where ceph containers are running in your
# test cluster: used for bouncing the containers.
NAMESPACE=rook-ceph

mkdir -p kubejacker
cp $SCRIPTPATH/Dockerfile kubejacker

# TODO: let user specify which daemon they're interested
# in -- doing all bins all the time is too slow and bloaty
#BINS="ceph-mgr ceph-mon ceph-mds ceph-osd rados radosgw-admin radosgw"
#pushd bin
#strip $BINS  #TODO: make stripping optional
#tar czf $BUILDPATH/kubejacker/bin.tar.gz $BINS
#popd

# We need ceph-common to support the binaries
# We need librados/rbd to support mgr modules
# that import the python bindings
#LIBS="libceph-common.so.0 libceph-common.so librados.so.2 librados.so librados.so.2.0.0 librbd.so librbd.so.1 librbd.so.1.12.0"
#pushd lib
#strip $LIBS  #TODO: make stripping optional
#tar czf $BUILDPATH/kubejacker/lib.tar.gz $LIBS
#popd

pushd ../src/pybind/mgr
find ./ -name "*.pyc" -exec rm -f {} \;
# Exclude node_modules because it's the huge sources in dashboard/frontend
tar --exclude=node_modules --exclude=tests --exclude-backups -czf $BUILDPATH/kubejacker/mgr_plugins.tar.gz *
popd

#ECLIBS="libec_*.so*"
#pushd lib
#strip $ECLIBS  #TODO: make stripping optional
#tar czf $BUILDPATH/kubejacker/eclib.tar.gz $ECLIBS
#popd

#CLSLIBS="libcls_*.so*"
#pushd lib
#strip $CLSLIBS  #TODO: make stripping optional
#tar czf $BUILDPATH/kubejacker/clslib.tar.gz $CLSLIBS
#popd

pushd kubejacker
docker build -t $REPO/ceph/ceph:latest .
popd

# Push the image to the repository
#docker tag $REPO/$IMAGE:$TAG $REPO/$IMAGE:latest
docker push $REPO/ceph/ceph:latest
#docker push $REPO/$IMAGE:$TAG

# Finally, bounce the containers to pick up the new image
kubectl -n $NAMESPACE delete pod -l app=rook-ceph-mds
kubectl -n $NAMESPACE delete pod -l app=rook-ceph-mgr
kubectl -n $NAMESPACE delete pod -l app=rook-ceph-mon
