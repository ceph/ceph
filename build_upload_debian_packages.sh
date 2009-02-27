#!/bin/sh

vers=`grep AM_INIT_AUTOMAKE configure.ac | head -1 | cut '-d '  -f 2 | sed 's/)//'`
echo vers $vers

repo=$1
arch=$2

rm -r ceph-$vers
make dist
tar zxvf ceph-$vers.tar.gz
cd ceph-$vers
./autogen.sh
dpkg-buildpackage -rfakeroot
cd ..

# upload
rsync -v --progress *$arch.{deb,changes} ceph.newdream.net:debian/dists/$repo/main/binary-$arch
rsync -v --progress ceph_* ceph.newdream.net:debian/dists/$repo/main/source

# rebuild index
ssh ceph.newdream.net build_debian_repo.sh