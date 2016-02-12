# lrc plugin
noinst_HEADERS += \
  erasure-code/lrc/ErasureCodeLrc.h

lrc_sources = \
  erasure-code/ErasureCode.cc \
  erasure-code/lrc/ErasureCodePluginLrc.cc \
  erasure-code/lrc/ErasureCodeLrc.cc

erasure-code/lrc/ErasureCodePluginLrc.cc: ./ceph_ver.h

libceph_lrc_la_SOURCES = ${lrc_sources} common/str_map.cc
libceph_lrc_la_CFLAGS = ${AM_CFLAGS}
libceph_lrc_la_CXXFLAGS= ${AM_CXXFLAGS}
libceph_lrc_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(LIBJSON_SPIRIT)
libceph_lrc_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libceph_lrc_la_LDFLAGS += -export-symbols-regex '.*__ceph_plugin_.*'
endif

erasure_codelib_LTLIBRARIES += libceph_lrc.la
