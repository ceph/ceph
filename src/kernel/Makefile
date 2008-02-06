#
# Makefile for CEPH filesystem.
#

obj-$(CONFIG_CEPH_FS) += ceph.o

ceph-objs := super.o inode.o dir.o file.o addr.o \
	ktcp.o messenger.o \
	client.o \
	mds_client.o mdsmap.o \
	mon_client.o \
	osd_client.o osdmap.o crush/crush.o crush/mapper.o \
	proc.o
