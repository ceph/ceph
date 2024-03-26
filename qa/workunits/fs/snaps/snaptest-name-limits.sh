#!/bin/bash
#
# This tests snapshot names limits: names have to be < 240 chars
#

function cleanup ()
{
	rmdir d1/.snap/*
	rm -rf d1
}

function fail ()
{
	echo $@
	cleanup
	exit 1
}

mkdir d1

longname=$(printf "%.241d" 2)
mkdir d1/.snap/$longname 2> /dev/null
[ -d d1/.snap/$longname ] && fail "Invalid snapshot exists: $longname"

cleanup

echo OK
