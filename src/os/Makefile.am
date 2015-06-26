include os/filestore/Makefile.am
include os/keyvaluestore/Makefile.am
include os/memstore/Makefile.am

if ENABLE_SERVER

noinst_HEADERS += \
	os/ObjectStore.h

libceph_os_la_SOURCES = \
	os/ObjectStore.cc \
	os/Transaction.cc \
	common/TrackedOp.cc

libceph_os_la_CXXFLAGS = ${AM_CXXFLAGS}
libceph_os_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS) $(LIBKVSTORE)

libceph_os_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0

lib_LTLIBRARIES += libceph_os.la

endif # ENABLE_SERVER
