compressorlibdir = $(pkglibdir)/compressor
compressorlib_LTLIBRARIES =  

include compressor/zlib/Makefile.am
include compressor/snappy/Makefile.am

libcompressor_la_SOURCES = \
	compressor/Compressor.cc \
	compressor/AsyncCompressor.cc
compressor/CompressionPlugin.cc: ./ceph_ver.h
libcompressor_la_DEPENDENCIES = $(compressorlib_LTLIBRARIES)
if LINUX
libcompressor_la_LIBADD = -ldl
endif # LINUX
noinst_LTLIBRARIES += libcompressor.la

noinst_HEADERS += \
	compressor/Compressor.h \
	compressor/AsyncCompressor.h \
    compressor/CompressionPlugin.h


EXTRA_DIST += compressor/CMakeLists.txt
