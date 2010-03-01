#!/bin/bash

set -e

remote=ceph.newdream.net
vers=$1
[ -z "$vers" ] && echo specify version && exit 1
[ ! -d release/$vers ] && echo release/$vers dne && exit 1

shift

if [ -z "$*" ]; then
    echo pushing $vers
    rsync -auv release/$vers/ $remote:release/$vers
else
    for e in $*; do
	echo pushing $vers/*.$e
	rsync -auv release/$vers/*.$e $remote:release/$vers
    done
fi
