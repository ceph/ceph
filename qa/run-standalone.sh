#!/bin/sh -ex

if [ ! -e Makefile ]; then
    echo 'run this from the build dir'
    exit 1
fi

for f in `find ../qa//standalone -executable -type f`
do
    echo '--- $f ---'
    PATH=$PATH:bin \
	CEPH_ROOT=.. \
	CEPH_LIB=lib \
	$f || exit 1
done

exit 0
