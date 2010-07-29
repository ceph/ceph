#!/bin/bash

set -e

remote=ceph.newdream.net
vers=$1
[ -z "$vers" ] && [ -e .last_release ] && vers=`cat .last_release`
[ -z "$vers" ] && echo specify version && exit 1
echo version $vers

test -d release/$vers || mkdir -p release/$vers

shift

if [ -z "$*" ]; then
    echo fetching $vers
    rsync -auv sage@$remote:release/$vers/ release/$vers
else
    for e in $*; do
	echo fetching $vers/*.$e
	rsync -auv sage@$remote:release/$vers/\*.$e release/$vers
    done
fi
