#!/bin/bash

set -e

vers=`grep AM_INIT_AUTOMAKE configure.ac | head -1 | cut '-d '  -f 2 | sed 's/)//'`
echo vers $vers

repo=$1
force=$2

[ -z "$repo" ] && echo stable or testing or unstable && exit 1

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

if [ "$repo" = "testing" ]; then
    versuffix=`date "+%Y%m%d%H%M"`
    finalvers="${vers}-testing${versuffix}-$gitver"
else
    if [ "$repo" = "unstable" ]; then
	versuffix=`date "+%Y%m%d%H%M"`
	finalvers="${vers}-unstable${versuffix}-$gitver"
    else
	finalvers="$vers"
    fi
fi

echo final vers $finalvers

echo making sure .git_version is up to date
cd src
./check_version .git_version
cd ..

echo building tarball
make dist

echo extracting
mkdir -p release/$finalvers
cd release/$finalvers

tar zxf ../../ceph-$vers.tar.gz 
[ "$vers" != "$finalvers" ] && mv ceph-$vers ceph-$finalvers
tar zcf ceph_$finalvers.orig.tar.gz ceph-$finalvers

# add debian dir
cp -a ../../debian ceph-$finalvers

for dist in sid squeeze lenny
do
    echo building $dist dsc
#    mkdir $dist
#    cd $dist

    dvers="$finalvers-1"
    [ "$dist" = "squeeze" ] && dvers="$dvers~bpo60+1"
    [ "$dist" = "lenny" ] && dvers="$dvers~bpo50+1"

    comment=""
    [ -n "$versuffix" ] && comment="git snapshot "
    [ "$dist" != "sid" ] && comment="${comment}$dist backport"

    if [ -n "$comment" ]; then
	cd ceph-$finalvers
	DEBEMAIL="sage@newdream.net" dch -D $dist --force-distribution -b -v "$dvers" "$comment"
	cd ..
    fi

    dpkg-source -b ceph-$finalvers

#    cd ..
done

rm -r ceph-$finalvers
cp -a ceph_$finalvers.orig.tar.gz ceph-$finalvers.tar.gz
echo finished release $finalvers

cd ../..
echo $finalvers > .last_release
