# pyramid plugin
libec_pyramid_la_SOURCES = \
  osd/ErasureCodePluginPyramid/ErasureCodePluginPyramid.cc \
  osd/ErasureCodePluginPyramid/ErasureCodePyramid.cc

noinst_HEADERS += \
  osd/ErasureCodePluginPyramid/ErasureCodePyramid.h

libec_pyramid_la_CFLAGS = ${AM_CFLAGS} 
libec_pyramid_la_CXXFLAGS= ${AM_CXXFLAGS} 
libec_pyramid_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS) $(LIBJSON_SPIRIT)
libec_pyramid_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
libec_pyramid_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_pyramid.la
