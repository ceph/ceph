# LRC plugin
noinst_HEADERS += \
  erasure-code/LRC/ErasureCodeLRC.h

LRC_sources = \
  erasure-code/ErasureCode.cc \
  erasure-code/LRC/ErasureCodePluginLRC.cc \
  erasure-code/LRC/ErasureCodeLRC.cc

erasure-code/LRC/ErasureCodePluginLRC.cc: ./ceph_ver.h

libec_LRC_la_SOURCES = ${LRC_sources} common/str_map.cc
libec_LRC_la_CFLAGS = ${AM_CFLAGS} 
libec_LRC_la_CXXFLAGS= ${AM_CXXFLAGS} 
libec_LRC_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(LIBJSON_SPIRIT)
libec_LRC_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
libec_LRC_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_LRC.la
