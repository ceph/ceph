#!/bin/sh -ex

if [ ! -e Makefile ]; then
    echo 'run this from the build dir'
    exit 1
fi

if [ `uname` = FreeBSD ]; then
    # otherwise module prettytable will not be found
    export PYTHONPATH=/usr/local/lib/python2.7/site-packages
    exec_mode=+111
else
    exec_mode=/111
fi

for f in `find ../qa/standalone -perm $exec_mode -type f`
do
    echo '--- $f ---'
    PATH=$PATH:bin \
	CEPH_ROOT=.. \
	CEPH_LIB=lib \
	$f || exit 1
done

exit 0
