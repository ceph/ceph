#!/bin/bash

set -e

basedir=~/debian-base

vers=$1
[ -z "$vers" ] && [ -e .last_release ] && vers=`cat .last_release`
[ -z "$vers" ] && echo specify version && exit 1

echo version $vers

#./pull.sh $vers gz dsc

for dist in sid squeeze lenny
do
    pbuilder --clean

    dvers="$vers-1"
    [ "$dist" = "squeeze" ] && dvers="$dvers~bpo60+1"
    [ "$dist" = "lenny" ] && dvers="$dvers~bpo50+1"
    echo debian vers $dvers

    echo building debs for $dist
    pbuilder build \
	--binary-arch \
	--basetgz $basedir/$dist.tgz --distribution $dist \
	--buildresult release/$vers \
	--debbuildopts -j`grep -c processor /proc/cpuinfo` \
	release/$vers/ceph_$dvers.dsc
    
done


# do lintian checks
for dist in sid squeeze lenny
do
    dvers="$vers-1"
    [ "$dist" = "squeeze" ] && dvers="$dvers~bpo60+1"
    [ "$dist" = "lenny" ] && dvers="$dvers~bpo50+1"
    echo lintian checks for $dvers
    lintian --allow-root release/$vers/*$dvers*.deb
done

