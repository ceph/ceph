#!/bin/sh

for dist in sid squeeze lenny
do
    pbuilder --clean
    
    if [ -e $basedir/$dist.tgz ]; then
	echo updating $dist base.tgz
	savelog -l -n  $basedir/$dist.tgz
	pbuilder update --basetgz $basedir/$dist.tgz --distribution $dist
    else
	echo building $dist base.tgz
	pbuilder create --basetgz $basedir/$dist.tgz --distribution $dist --mirror http://http.us.debian.org/debian
    fi
done

