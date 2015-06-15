if ENABLE_SERVER

libceph_memstore_la_SOURCES = \
	os/memstore/MemStore.cc \
	os/memstore/MemStorePlugin.cc

libceph_memstore_la_CXXFLAGS = ${AM_CXXFLAGS}
libceph_memstore_la_LIBADD = $(LIBCOMMON) $(PTHREAD_LIBS) $(EXTRALIBS)
#if WITH_LTTNG
#libceph_memstore_la_LIBADD += $(LIBOS_TP)
#endif

libceph_memstore_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
libceph_memstore_la_LDFLAGS += -export-symbols-regex '.*__ceph_plugin_.*'
endif


noinst_HEADERS += \
	os/ObjectStore.h \
	os/memstore/MemStore.h

lib_LTLIBRARIES += libceph_memstore.la
endif # ENABLE_SERVER
