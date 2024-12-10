#!/bin/sh

set -x
set -e

seed=$1
from=$2
to=$3
dir=$4

mydir=`dirname $0`

for f in `seq $from $to`
do
    if ! $mydir/run_seed_to.sh -o 10 -e $seed $f; then
	if [ -d "$dir" ]; then
	    echo copying evidence to $dir
	    cp -a . $dir
	else
	    echo no dir provided for evidence disposal
	fi
	exit 1
    fi
done
