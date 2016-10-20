# zlib plugin
noinst_HEADERS += \
  compressor/zlib/CompressionZlib.h

zlib_sources = \
  compressor/Compressor.cc \
  compressor/zlib/CompressionPluginZlib.cc \
  compressor/zlib/CompressionZlib.cc

compressor/zlib/CompressionPluginZlib.cc: ./ceph_ver.h

libceph_zlib_la_SOURCES = ${zlib_sources}
libceph_zlib_la_CFLAGS = ${AM_CFLAGS}  
libceph_zlib_la_CXXFLAGS= ${AM_CXXFLAGS} 
libceph_zlib_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libceph_zlib_la_LDFLAGS = ${AM_LDFLAGS} -lz -version-info 2:0:0
if LINUX
libceph_zlib_la_LDFLAGS += -export-symbols-regex '.*__compressor_.*'
endif

compressorlib_LTLIBRARIES += libceph_zlib.la

EXTRA_DIST += compressor/zlib/CMakeLists.txt
