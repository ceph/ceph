#!/bin/sh

#
# Create a SRPM which can be used to build Ceph
#
# ./make-srpm.sh <version>
# rpmbuild --rebuild /tmp/ceph/ceph-<version>-0.el7.centos.src.rpm
#

./make-dist $1
rpmbuild -D"_sourcedir `pwd`" -D"_specdir `pwd`" -D"_srcrpmdir `pwd`" -bs ceph.spec
