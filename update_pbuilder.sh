#!/bin/sh -x

basedir=~/debian-base
dist=$1
os=$2
[ -z "$os" ] && os="debian"

if [ $os = "debian" ]; then
    mirror="http://http.us.debian.org/debian"
    othermirror=""
else
    mirror=""
    othermirror="deb http://archive.ubuntu.com/ubuntu $dist main restricted universe multiverse"
fi

pbuilder --clean
    
if [ -e $basedir/$dist.tgz ]; then
    echo updating $dist base.tgz
    savelog -l -n  $basedir/$dist.tgz
    cp $basedir/$dist.tgz.0 $basedir/$dist.tgz
    pbuilder update --basetgz $basedir/$dist.tgz --distribution $dist #--mirror "$mirror" --othermirror "$othermirror"
else
    echo building $dist base.tgz
    pbuilder create --basetgz $basedir/$dist.tgz --distribution $dist --mirror "$mirror" --othermirror "$othermirror"
fi

