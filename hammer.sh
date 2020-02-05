#!/bin/sh -ex
#
# simple script to repeat a test until it fails
#

if [ $1 = "-a" ]; then
    shift
    job=$1
    log="--archive $job.out"
else
    job=$1
    log=""
fi

test -e $1

title() {
	echo '\[\033]0;hammer '$job' '$N' passes\007\]'
}

N=0
title
[ -n "$log" ] && [ -d $job.out ] && rm -rf $job.out
while teuthology $log $job $2 $3 $4 
do
	date
	N=$(($N+1))
	echo "$job: $N passes"
	[ -n "$log" ] && rm -rf $job.out
	title
done
echo "$job: $N passes, then failure."
