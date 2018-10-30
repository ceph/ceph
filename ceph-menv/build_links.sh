#!/bin/bash

DIR=`dirname $0`
ROOT=$1

[ "$ROOT" == "" ] && ROOT="$HOME/ceph"

mkdir -p $DIR/bin

echo $PWD
for f in `ls $ROOT/build/bin`; do
        echo $f
        ln -sf ../mdo.sh $DIR/bin/$f
done

echo "MRUN_CEPH_ROOT=$ROOT" > $DIR/.menvroot
