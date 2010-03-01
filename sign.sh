#!/bin/bash

set -e

vers=$1
[ -z "$vers" ] && echo specify version && exit 1

./pull.sh $vers dsc changes

for f in `cd release/$vers ; ls *.{dsc,changes}`
do
    if [ -e "release/$vers/$f" ]; then
	if head -1 release/$vers/$f | grep -q 'BEGIN PGP SIGNED MESSAGE' ; then
	    echo already signed $f
	else
	    debsign release/$vers/$f
	fi
    fi
done