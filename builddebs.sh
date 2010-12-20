#!/bin/bash

set -e

basedir=~/debian-base

vers=$1
debsubver=$2
dists=$3

[ -z "$debsubver" ] && debsubver="1"

[ -z "$vers" ] && [ -e .last_release ] && vers=`cat .last_release`
[ -z "$vers" ] && echo specify version && exit 1

echo version $vers

#./pull.sh $vers gz dsc

[ -z "$dists" ] && dists="sid squeeze lenny"

for dist in $dists
do
    pbuilder --clean

    dvers="$vers-$debsubver"
    [ "$dist" = "squeeze" ] && dvers="$dvers~bpo60+1"
    [ "$dist" = "lenny" ] && dvers="$dvers~bpo50+1"
    echo debian vers $dvers

    echo building debs for $dist

    cmd="pbuilder build \
	--binary-arch \
	--basetgz $basedir/$dist.tgz --distribution $dist \
	--buildresult release/$vers \
	--debbuildopts -j`grep -c processor /proc/cpuinfo` \
	release/$vers/ceph_$dvers.dsc"

    if $cmd ; then
	echo $dist done
    else
	./update_pbuilder.sh $dist
	$cmd
    fi
done


# do lintian checks
for dist in $dists
do
    dvers="$vers-$debsubver"
    [ "$dist" = "squeeze" ] && dvers="$dvers~bpo60+1"
    [ "$dist" = "lenny" ] && dvers="$dvers~bpo50+1"
    echo lintian checks for $dvers
    lintian --allow-root release/$vers/*$dvers*.deb
done

