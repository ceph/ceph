#!/bin/sh

./make-dist
rpmbuild -D"_sourcedir `pwd`" -D"_specdir `pwd`" -D"_srcrpmdir `pwd`" -bs ceph.spec
