#!/bin/sh -x

basedir=~/debian-base
dists=$1

[ -z "$dists" ] && dists="sid squeeze lenny maverick lucid"

for dist in $dists
do
    os="debian"
    [ "$dist" == "maverick" ] && os="ubuntu"
    [ "$dist" == "lucid" ] && os="ubuntu"
    
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
done