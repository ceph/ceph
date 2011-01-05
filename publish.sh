#!/bin/bash

set -e

repo=~/debian

vers=$1
debsubver=$2

[ -z "$debsubver" ] && debsubver="1"

[ -z "$vers" ] && [ -e .last_release ] && vers=`cat .last_release`
[ -z "$vers" ] && echo specify version && exit 1
[ ! -d "release/$vers" ] && echo missing release/$vers && exit 1
echo version $vers

if echo $vers | grep -q testing ; then
    component="ceph-testing"
else
    if echo $vers | grep -q unstable ; then
	component="ceph-unstable"
    else
	if echo $vers | grep -q rc ; then
	    component="ceph-rc"
	else
	    component="ceph-stable"
	fi
    fi
fi
echo component $component

for dist in sid squeeze lenny maverick lucid
do
    dvers="$vers-$debsubver"
    [ "$dist" = "squeeze" ] && dvers="$dvers~bpo60+1"
    [ "$dist" = "lenny" ] && dvers="$dvers~bpo50+1"
    echo debian dist $dist vers $dvers

    for f in release/$vers/ceph_${dvers}_*.changes
    do
	reprepro --ask-passphrase -b $repo -C $component --ignore=wrongdistribution include $dist $f
    done
done
