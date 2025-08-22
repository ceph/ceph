#!/bin/sh

#
# Create a SRPM which can be used to build Ceph
#
# ./make-srpm.sh <version>
# rpmbuild --rebuild /tmp/ceph/ceph-<version>-0.el7.centos.src.rpm
#

test -f "ceph-$1.tar.bz2" || ./make-dist $1
rpmbuild -D"_sourcedir ${PWD}" -D"_specdir ${PWD}" -D"_srcrpmdir ${PWD}" -bs ceph.spec
