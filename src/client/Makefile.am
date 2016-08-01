if ENABLE_CLIENT
libclient_la_SOURCES = \
	client/Client.cc \
	client/Inode.cc \
	client/Dentry.cc \
	client/MetaRequest.cc \
	client/ClientSnapRealm.cc \
	client/MetaSession.cc \
	client/Trace.cc \
	client/posix_acl.cc
libclient_la_LIBADD = $(LIBOSDC)
noinst_LTLIBRARIES += libclient.la

noinst_HEADERS += \
	client/Client.h \
	client/Dentry.h \
	client/Dir.h \
	client/Fh.h \
	client/Inode.h \
	client/InodeRef.h \
	client/MetaRequest.h \
	client/MetaSession.h \
	client/ClientSnapRealm.h \
	client/SyntheticClient.h \
	client/Trace.h \
	client/ioctl.h \
	client/ObjecterWriteback.h \
	client/posix_acl.h \
	client/UserGroups.h

if WITH_FUSE
libclient_fuse_la_SOURCES = client/fuse_ll.cc
libclient_fuse_la_LIBADD = libclient.la $(LIBFUSE_LIBS)
libclient_fuse_la_CXXFLAGS = $(AM_CXXFLAGS) $(LIBFUSE_CFLAGS)
noinst_LTLIBRARIES += libclient_fuse.la
noinst_HEADERS += client/fuse_ll.h
endif

ceph_test_ioctls_SOURCES = client/test_ioctls.c
bin_DEBUGPROGRAMS += ceph_test_ioctls

endif # ENABLE_CLIENT
