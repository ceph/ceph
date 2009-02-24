#!/bin/sh

repo=$1
arch=$2

rm ceph-*.tar.gz
rm -r ceph-0.?
make dist
tar zxvf ceph-*.tar.gz
cd ceph-0.?
./autogen.sh
dpkg-buildpackage -rfakeroot
cd ..

rsync -v --progress *$arch.{deb,changes} ceph.newdream.net:debian/dists/$repo/main/binary-$arch
rsync -v --progress ceph_* ceph.newdream.net:debian/dists/$repo/main/source

# rebuild index
ssh ceph.newdream.net build_debian_repo.sh