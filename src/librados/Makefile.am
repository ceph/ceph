librados_la_SOURCES = \
	librados/librados.cc \
	librados/RadosClient.cc \
	librados/IoCtxImpl.cc \
	librados/snap_set_diff.cc \
	librados/RadosXattrIter.cc

# We need this to avoid basename conflicts with the librados build tests in test/Makefile.am
librados_la_CXXFLAGS = ${AM_CXXFLAGS}

LIBRADOS_DEPS += libcls_lock_client.la $(LIBOSDC) $(LIBCOMMON)
if WITH_LTTNG
LIBRADOS_DEPS += $(LIBRADOS_TP)
endif

librados_la_LIBADD = $(LIBRADOS_DEPS) $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
librados_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
if LINUX
librados_la_LDFLAGS += -export-symbols-regex '^rados_.*'
endif
lib_LTLIBRARIES += librados.la

noinst_HEADERS += \
	librados/snap_set_diff.h \
	librados/AioCompletionImpl.h \
	librados/IoCtxImpl.h \
	librados/PoolAsyncCompletionImpl.h \
	librados/RadosClient.h \
	librados/RadosXattrIter.h \
	librados/ListObjectImpl.h
