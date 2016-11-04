if ENABLE_CLIENT
if WITH_RADOS
if WITH_RADOSSTRIPER

libradosstriper_la_SOURCES = \
	libradosstriper/libradosstriper.cc \
	libradosstriper/RadosStriperImpl.cc \
	libradosstriper/MultiAioCompletionImpl.cc

# We need this to avoid basename conflicts with the libradosstriper build tests in test/Makefile.am
libradosstriper_la_CXXFLAGS = ${AM_CXXFLAGS}

LIBRADOSSTRIPER_DEPS = librados_internal.la libcls_lock_client.la $(LIBOSDC) $(LIBCOMMON_DEPS)
libradosstriper_la_LIBADD = $(LIBRADOSSTRIPER_DEPS) $(LIBRADOS) $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
libradosstriper_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
libradosstriper_la_LDFLAGS += -export-symbols-regex '^radosstriper_.*'
endif
lib_LTLIBRARIES += libradosstriper.la

noinst_HEADERS += \
	libradosstriper/RadosStriperImpl.h \
	libradosstriper/MultiAioCompletionImpl.h

endif # WITH_RADOSSTRIPER
endif # WITH_RADOS
endif # ENABLE_CLIENT

EXTRA_DIST += libradosstriper/CMakeLists.txt
