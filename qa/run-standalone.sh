#!/bin/sh -ex

if [ ! -e Makefile ]; then
    echo 'run this from the build dir'
    exit 1
fi

if [ `uname` = FreeBSD ]; then
  # otherwise module prettytable will not be found
  export PYTHONPATH=/usr/local/lib/python2.7/site-packages
fi

for f in `find ../qa/standalone -perm +111 -type f`
do
    echo '--- $f ---'
    PATH=$PATH:bin \
	CEPH_ROOT=.. \
	CEPH_LIB=lib \
	$f || exit 1
done

exit 0
