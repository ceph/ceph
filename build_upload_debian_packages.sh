#!/bin/sh

vers=`grep AM_INIT_AUTOMAKE configure.ac | head -1 | cut '-d '  -f 2 | sed 's/)//'`
echo vers $vers

repo=$1
arch=$2
snapshot=$3

if [ "$repo" = "unstable" ]; then
    versuffix=`date "+%Y%m%d%H%M%S"`
    finalvers="$vers.$versuffix"
    debdate=`date "+%a, %d %b %Y %X %z"`
else
    finalvers="$vers"
fi

echo final vers $finalvers

echo cleanup
rm *.deb *.tar.gz *.changes *.dsc
rm -rf ceph-$vers*

echo building tarball
make dist

echo extracting
tar zxf ceph-$vers.tar.gz

if [ "$vers" != "$finalvers" ]; then
    echo "renaming ceph-$vers to ceph-$finalvers, rebuilding tarball"
    mv ceph-$vers ceph-$finalvers
    sed -i "s/ceph, $vers/ceph, $finalvers/" ceph-$finalvers/configure.ac

    mv ceph-$finalvers/debian/changelog ceph-$finalvers/debian/changelog.tmp
    cat <<EOF > ceph-$finalvers/debian/changelog
ceph ($finalvers) unstable; urgency=low

   * snapshot from git at $versuffix

 -- sage <sage@newdream.net>  $debdate

EOF
    cat ceph-$finalvers/debian/changelog.tmp >> ceph-$finalvers/debian/changelog
    
    tar zcf ceph-$finalvers.tar.gz ceph-$finalvers
fi;

cd ceph-$finalvers
./autogen.sh
dpkg-buildpackage -rfakeroot
cd ..

# upload
rsync -v --progress *$arch.{deb,changes} sage@ceph.newdream.net:debian/dists/$repo/main/binary-$arch
rsync -v --progress ceph_* sag@ceph.newdream.net:debian/dists/$repo/main/source

# rebuild index
ssh sage@ceph.newdream.net build_debian_repo.sh