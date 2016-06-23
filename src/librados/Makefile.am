if ENABLE_CLIENT
if WITH_RADOS

librados_internal_la_SOURCES = \
	librados/RadosClient.cc \
	librados/IoCtxImpl.cc \
	librados/RadosXattrIter.cc \
	librados/snap_set_diff.cc
noinst_LTLIBRARIES += librados_internal.la

librados_api_la_SOURCES = \
	common/buffer.cc \
	librados/librados.cc
noinst_LTLIBRARIES += librados_api.la

librados_la_SOURCES = \
	common/buffer.cc \
	librados/librados.cc

# We need this to avoid basename conflicts with the librados build tests in test/Makefile.am
librados_la_CXXFLAGS = ${AM_CXXFLAGS}

LIBRADOS_DEPS += \
	librados_internal.la libcls_lock_client.la \
	$(LIBOSDC) $(LIBCOMMON)

librados_la_LIBADD = $(LIBRADOS_DEPS) $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
librados_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
if LINUX
librados_la_CXXFLAGS += -fvisibility=hidden -fvisibility-inlines-hidden
librados_la_LDFLAGS += -Xcompiler -Xlinker -Xcompiler '--exclude-libs=ALL'
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

endif # WITH_RADOS
endif # ENABLE_CLIENT
