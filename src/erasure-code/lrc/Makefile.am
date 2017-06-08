# lrc plugin
noinst_HEADERS += \
  erasure-code/lrc/ErasureCodeLrc.h

lrc_sources = \
  erasure-code/ErasureCode.cc \
  erasure-code/lrc/ErasureCodePluginLrc.cc \
  erasure-code/lrc/ErasureCodeLrc.cc

erasure-code/lrc/ErasureCodePluginLrc.cc: ./ceph_ver.h

libec_lrc_la_SOURCES = ${lrc_sources} common/str_map.cc
libec_lrc_la_CFLAGS = ${AM_CFLAGS}
libec_lrc_la_CXXFLAGS= ${AM_CXXFLAGS}
libec_lrc_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(LIBJSON_SPIRIT)
libec_lrc_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_lrc_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_lrc.la
