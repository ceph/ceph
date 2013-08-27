librados_la_SOURCES = \
	librados/librados.cc \
	librados/RadosClient.cc \
	librados/IoCtxImpl.cc \
	librados/snap_set_diff.cc

# We need this to avoid basename conflicts with the librados build tests in test/Makefile.am
librados_la_CFLAGS = ${AM_CFLAGS}

librados_la_LIBADD = libcls_lock_client.la $(LIBOSDC) $(LIBCOMMON) $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
librados_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0 -export-symbols-regex '^rados_.*'
lib_LTLIBRARIES += librados.la

noinst_HEADERS += \
	librados/snap_set_diff.h \
	librados/AioCompletionImpl.h \
	librados/IoCtxImpl.h \
	librados/PoolAsyncCompletionImpl.h \
	librados/RadosClient.h
