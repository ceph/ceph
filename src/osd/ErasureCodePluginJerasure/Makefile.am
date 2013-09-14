# jerasure plugin
libec_jerasure_la_SOURCES = \
  osd/ErasureCodePluginJerasure/ErasureCodePluginJerasure.cc \
  osd/ErasureCodePluginJerasure/ErasureCodeJerasure.cc \
  osd/ErasureCodePluginJerasure/cauchy.c \
  osd/ErasureCodePluginJerasure/galois.c \
  osd/ErasureCodePluginJerasure/jerasure.c \
  osd/ErasureCodePluginJerasure/liberation.c \
  osd/ErasureCodePluginJerasure/reed_sol.c
noinst_HEADERS += \
  osd/ErasureCodePluginJerasure/ErasureCodeJerasure.h \
  osd/ErasureCodePluginJerasure/cauchy.h \
  osd/ErasureCodePluginJerasure/galois.h \
  osd/ErasureCodePluginJerasure/jerasure.h \
  osd/ErasureCodePluginJerasure/liberation.h \
  osd/ErasureCodePluginJerasure/reed_sol.h
libec_jerasure_la_CFLAGS = ${AM_CFLAGS} 
libec_jerasure_la_CXXFLAGS= ${AM_CXXFLAGS} 
libec_jerasure_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__erasure_code_.*'

erasure_codelib_LTLIBRARIES += libec_jerasure.la
