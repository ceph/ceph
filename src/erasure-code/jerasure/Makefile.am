# jerasure plugin
libec_jerasure_la_SOURCES = \
  erasure-code/jerasure/ErasureCodePluginJerasure.cc \
  erasure-code/jerasure/ErasureCodeJerasure.cc \
  erasure-code/jerasure/cauchy.c \
  erasure-code/jerasure/galois.c \
  erasure-code/jerasure/jerasure.c \
  erasure-code/jerasure/liberation.c \
  erasure-code/jerasure/reed_sol.c

noinst_HEADERS += \
  erasure-code/jerasure/ErasureCodeJerasure.h \
  erasure-code/jerasure/cauchy.h \
  erasure-code/jerasure/galois.h \
  erasure-code/jerasure/jerasure.h \
  erasure-code/jerasure/liberation.h \
  erasure-code/jerasure/reed_sol.h \
  erasure-code/jerasure/vectorop.h

libec_jerasure_la_CFLAGS = ${AM_CFLAGS} 
libec_jerasure_la_CXXFLAGS= ${AM_CXXFLAGS} 
libec_jerasure_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
libec_jerasure_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_jerasure.la
