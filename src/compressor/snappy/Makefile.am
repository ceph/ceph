# snappy plugin
noinst_HEADERS += \
  compressor/snappy/SnappyCompressor.h

snappy_sources = \
  compressor/Compressor.cc \
  compressor/snappy/CompressionPluginSnappy.cc

compressor/snappy/CompressionPluginSnappy.cc: ./ceph_ver.h

libceph_snappy_la_SOURCES = ${snappy_sources}
libceph_snappy_la_CFLAGS = ${AM_CFLAGS}  \
  -I$(srcdir)/compressor/snappy/snappy-1.2.8
libceph_snappy_la_CXXFLAGS= ${AM_CXXFLAGS} \
  -I$(srcdir)/compressor/snappy/snappy-1.2.8
libceph_snappy_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libceph_snappy_la_LDFLAGS = ${AM_LDFLAGS} -lsnappy -version-info 2:0:0
if LINUX
libceph_snappy_la_LDFLAGS += -export-symbols-regex '.*__compressor_.*'
endif

compressorlib_LTLIBRARIES += libceph_snappy.la
