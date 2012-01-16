#!/bin/sh -ex

job=$1
test -e $1

bin/teuthology-nuke -t $job

title() {
	echo '\[\033]0;hammer '$job' '$N' passes\007\]'
}

N=0
title
while bin/teuthology $job $2 $3 $4
do
	date
	N=$(($N+1))
	echo "$job: $N passes"
	title
done
echo "$job: $N passes, then failure."
