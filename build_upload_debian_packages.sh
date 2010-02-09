#!/bin/bash

set -e

vers=`grep AM_INIT_AUTOMAKE configure.ac | head -1 | cut '-d '  -f 2 | sed 's/)//'`
echo vers $vers

repo=$1
arch=$2

[ -z "$repo" ] && echo no repo && exit 1
[ -z "$arch" ] && echo no arch && exit 1

if [ "$repo" = "unstable" ]; then
    versuffix=`date "+%Y%m%d%H%M%S"`
#    if [ `echo $vers | sed 's/[^\.]//g'` = ".." ]; then
#	finalvers="$vers$versuffix"
#    else
#	finalvers="$vers.$versuffix"
#    fi
    finalvers="${vers}git$versuffix"
    debdate=`date "+%a, %d %b %Y %X %z"`
else
    finalvers="$vers"
fi

echo final vers $finalvers

echo cleanup
rm -rf ceph-* || true
rm -rf debtmp || true

echo generating git version stamp
cd src
./make_version
grep GIT_VER ceph_ver.h
cd ..

echo building tarball
make dist

echo extracting
tar zxf ceph-$vers.tar.gz

# mangle version
if [ "$vers" != "$finalvers" ]; then
    echo "renaming ceph-$vers to ceph-$finalvers, rebuilding tarball"
    mv ceph-$vers ceph-$finalvers
    sed -i "s/ceph, $vers/ceph, $finalvers/" ceph-$finalvers/configure.ac
   
    tar zcf ceph-$finalvers.tar.gz ceph-$finalvers
fi;


## go

echo creating debtmp with .orig.tar.gz
mkdir -p debtmp
cp ceph-$finalvers.tar.gz debtmp/ceph_$finalvers.orig.tar.gz
cd debtmp

echo extracting .orig.tar.gz
tar zxf ceph_$finalvers.orig.tar.gz

# copy in debian dir, fix up changelog
echo setting up debian dir
cp -aL ../debian ceph-$finalvers
if [ "$vers" != "$finalvers" ]; then
    cd ceph-$finalvers
    DEBEMAIL="sage@newdream.net" dch -v $finalvers-1 'git snapshot'
    cd ..
fi

cd ceph-$finalvers
dpkg-buildpackage -rfakeroot -us -uc
cd ..

if [ "$3" = "upload" ]; then

    # upload
    rsync -v --progress *$arch* sage@ceph.newdream.net:debian/dists/$repo/main/binary-$arch
    rsync -v --progress ceph_$finalvers[.-]* sage@ceph.newdream.net:debian/dists/$repo/main/source

    cd ..

#exit 0

    if [ "$repo" = "stable" -a "$arch" = "amd64" ]; then
	scp ceph-$vers.tar.gz sage@ceph.newdream.net:ceph.newdream.net/downloads
    fi

    # rebuild index
    ssh sage@ceph.newdream.net build_debian_repo.sh
fi