libclient_la_SOURCES = \
	client/Client.cc \
	client/Inode.cc \
	client/Dentry.cc \
	client/MetaRequest.cc \
	client/ClientSnapRealm.cc \
	client/MetaSession.cc \
	client/Trace.cc
libclient_la_LIBADD = $(LIBOSDC) $(LIBEDIT_LIBS)
noinst_LTLIBRARIES += libclient.la

noinst_HEADERS += \
	client/Client.h \
	client/Dentry.h \
	client/Dir.h \
	client/Fh.h \
	client/Inode.h \
	client/MetaRequest.h \
	client/MetaSession.h \
	client/ClientSnapRealm.h \
	client/SyntheticClient.h \
	client/Trace.h \
	client/ioctl.h \
	client/ObjecterWriteback.h

if WITH_HADOOPCLIENT
JAVA_BASE = /usr/lib/jvm/java-6-sun
libhadoopcephfs_la_SOURCES = client/hadoop/CephFSInterface.cc
libhadoopcephfs_la_LIBADD = $(LIBCEPHFS)
libhadoopcephfs_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex 'hadoopcephfs_.*'
lib_LTLIBRARIES += libhadoopcephfs.la
noinst_HEADERS += client/hadoop/CephFSInterface.h
endif # WITH_HADOOPCLIENT

if WITH_FUSE
libclient_fuse_la_SOURCES = client/fuse_ll.cc
libclient_fuse_la_LIBADD = libclient.la -lfuse
noinst_LTLIBRARIES += libclient_fuse.la
noinst_HEADERS += client/fuse_ll.h
endif
