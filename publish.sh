#!/bin/bash

set -e

repo=~/debian

vers=$1
[ -z "$vers" ] && echo specify version && exit 1
[ ! -d "release/$vers" ] && echo missing release/$vers && exit 1

if echo $vers | grep -q git ; then
    component="ceph-unstable"
else
    component="ceph-stable"
fi

echo vers $vers
echo component $component

for dist in sid squeeze lenny
do
    dvers="$vers-1"
    [ "$dist" = "squeeze" ] && dvers="$dvers~bpo60+1"
    [ "$dist" = "lenny" ] && dvers="$dvers~bpo50+1"
    echo debian dist $dist vers $dvers

    for f in release/$vers/ceph_${dvers}_*.changes
    do
	reprepro --ask-passphrase -b $repo -C $component --ignore=wrongdistribution include $dist $f
    done
done
