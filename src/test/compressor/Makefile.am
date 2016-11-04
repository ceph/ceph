if ENABLE_SERVER
if WITH_OSD


libceph_example_la_SOURCES = \
	compressor/Compressor.cc \
	test/compressor/compressor_plugin_example.cc
noinst_HEADERS += test/compressor/compressor_example.h
test/compressor/compressor_plugin_example.cc: ./ceph_ver.h
libceph_example_la_CFLAGS = ${AM_CFLAGS}
libceph_example_la_CXXFLAGS= ${AM_CXXFLAGS}
libceph_example_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libceph_example_la_LDFLAGS = ${AM_LDFLAGS} -export-symbols-regex '.*__compression_.*'
compressorlib_LTLIBRARIES += libceph_example.la

unittest_compression_plugin_SOURCES = \
	compressor/Compressor.cc \
	test/compressor/test_compression_plugin.cc 
unittest_compression_plugin_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_compression_plugin_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
if LINUX
unittest_compression_plugin_LDADD += -ldl
endif
check_TESTPROGRAMS += unittest_compression_plugin

unittest_compression_snappy_SOURCES = \
	test/compressor/test_compression_snappy.cc \
	${snappy_sources}
unittest_compression_snappy_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_compression_snappy_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_compression_snappy_LDFLAGS = -lsnappy
if LINUX
unittest_compression_snappy_LDADD += -ldl
endif
check_TESTPROGRAMS += unittest_compression_snappy

unittest_compression_plugin_snappy_SOURCES = \
	test/compressor/test_compression_plugin_snappy.cc \
	${snappy_sources}
unittest_compression_plugin_snappy_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_compression_plugin_snappy_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL) $(LIBCOMPRESSOR)
unittest_compression_plugin_snappy_LDFLAGS = -lsnappy
if LINUX
unittest_compression_plugin_snappy_LDADD += -ldl
endif
check_TESTPROGRAMS += unittest_compression_plugin_snappy

unittest_compression_zlib_SOURCES = \
	test/compressor/test_compression_zlib.cc \
	${zlib_sources}
unittest_compression_zlib_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_compression_zlib_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_compression_zlib_LDFLAGS = -lz
if LINUX
unittest_compression_zlib_LDADD += -ldl
endif
check_TESTPROGRAMS += unittest_compression_zlib

unittest_compression_plugin_zlib_SOURCES = \
	test/compressor/test_compression_plugin_zlib.cc \
	${zlib_sources}
unittest_compression_plugin_zlib_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_compression_plugin_zlib_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL) $(LIBCOMPRESSOR)
unittest_compression_plugin_zlib_LDFLAGS = -lz
if LINUX
unittest_compression_plugin_zlib_LDADD += -ldl
endif
check_TESTPROGRAMS += unittest_compression_plugin_zlib

endif # WITH_OSD
endif # ENABLE_SERVER

EXTRA_DIST += test/compressor/CMakeLists.txt
