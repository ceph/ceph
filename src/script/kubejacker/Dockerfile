FROM ceph/daemon-base:latest-master
# for openSUSE, use:
# FROM registry.opensuse.org/home/ssebastianwagner/rook-ceph/images/opensuse/leap:latest


#ADD bin.tar.gz /usr/bin/
#ADD lib.tar.gz /usr/lib64/

# Assume developer is using default paths (i.e. /usr/local), so
# build binaries will be looking for libs there.
#ADD eclib.tar.gz /usr/local/lib64/ceph/erasure-code/
#ADD clslib.tar.gz /usr/local/lib64/rados-classes/

ADD mgr_plugins.tar.gz /usr/share/ceph/mgr
