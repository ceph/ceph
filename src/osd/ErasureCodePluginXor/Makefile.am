# xor plugin
libec_xor_la_SOURCES = \
  osd/ErasureCodePluginXor/ErasureCodePluginXor.cc \
  osd/ErasureCodePluginXor/ErasureCodeXor.cc \
  osd/ErasureCodePluginXor/xor.cc 

noinst_HEADERS += \
  osd/ErasureCodePluginXor/ErasureCodeXor.h \
  osd/ErasureCodePluginXor/xor.h 

libec_xor_la_CFLAGS = ${AM_CFLAGS} 
libec_xor_la_CXXFLAGS= ${AM_CXXFLAGS} 
libec_xor_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libec_xor_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
libec_xor_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_xor.la
