#!/bin/bash -x

set -e

vers=`grep AM_INIT_AUTOMAKE configure.ac | head -1 | cut '-d '  -f 2 | sed 's/)//'`
echo vers $vers

repo=$1
debsubver=$2
force=$3

[ -z "$debsubver" ] && debsubver=1

[ -z "$repo" ] && echo stable or testing or unstable or rc && exit 1

if git diff --quiet ; then
    echo repository is clean
else
    echo
    echo "**** REPOSITORY IS DIRTY ****"
    echo
    if [ "$force" != "force" ]; then
	echo "add 'force' argument if you really want to continue."
	exit 1
    fi
    echo "forcing."
fi

gitver=`git rev-parse HEAD 2>/dev/null | cut -c 1-8`
echo gitver $gitver

if [ "$repo" = "stable" ]; then
    cephver="$vers"
else
    versuffix=`date "+%Y%m%d%H%M"`
    cephver="${vers}-$repo${versuffix}-$gitver"
fi

echo final vers $cephver

if [ -d "release/$cephver" ]; then
    echo "release/$cephver already exists; reusing that release tarball"
    cd release/$cephver
    tar zxvf ceph_$cephver.orig.tar.gz
else
    echo making sure .git_version is up to date
    cd src
    ./check_version .git_version
    cd ..
    
    echo building tarball
    make dist

    echo extracting
    mkdir -p release/$cephver
    cd release/$cephver

    tar zxf ../../ceph-$vers.tar.gz 
    [ "$vers" != "$cephver" ] && mv ceph-$vers ceph-$cephver
    tar zcf ceph_$cephver.orig.tar.gz ceph-$cephver
    cp -a ceph_$cephver.orig.tar.gz ceph-$cephver.tar.gz
fi

# add debian dir
echo "copying latest debian dir"
cp -a ../../debian ceph-$cephver

debver="$cephver-$debsubver"

for dist in sid squeeze lenny
do
    echo building $dist dsc
#    mkdir $dist
#    cd $dist

    bpver="$debver"
    [ "$dist" = "squeeze" ] && bpver="$debver~bpo60+1"
    [ "$dist" = "lenny" ] && bpver="$debver~bpo50+1"

    comment=""
#    [ "$debsubver" != "1" ] && comment="package fixes "
    [ -n "$versuffix" ] && comment="git snapshot "
    [ "$dist" != "sid" ] && comment="${comment}$dist backport"

    if [ -n "$comment" ]; then
	cd ceph-$cephver
	DEBEMAIL="sage@newdream.net" dch -D $dist --force-distribution -b -v "$bpver" "$comment"
	cd ..
    fi

    [ "$dist" = "lenny" ] && sed -i 's/, libgoogle-perftools-dev//' ceph-$cephver/debian/control

    dpkg-source -b ceph-$cephver

#    cd ..
done

rm -r ceph-$cephver
echo finished release $debver

cd ../..
echo $cephver > .last_release
