# SHEC plugin

libec_shec_la_SOURCES = \
	erasure-code/ErasureCode.cc \
	erasure-code/shec/ErasureCodePluginShec.cc \
	erasure-code/shec/ErasureCodeShec.cc \
	erasure-code/shec/shec.cc \
	erasure-code/shec/determinant.c \
	erasure-code/shec/jerasure/cauchy.c \
	erasure-code/shec/jerasure/galois.c \
	erasure-code/shec/jerasure/jerasure.c \
	erasure-code/shec/jerasure/liberation.c \
	erasure-code/shec/jerasure/reed_sol.c
noinst_HEADERS += \
	erasure-code/shec/ErasureCodeShec.h \
	erasure-code/shec/shec.h \
	erasure-code/shec/jerasure/cauchy.h \
	erasure-code/shec/jerasure/galois.h \
	erasure-code/shec/jerasure/jerasure.h \
	erasure-code/shec/jerasure/liberation.h \
	erasure-code/shec/jerasure/reed_sol.h

erasure-code/shec/ErasureCodePluginShec.cc: ./ceph_ver.h

libec_shec_la_CFLAGS = ${AM_CFLAGS} \
	-I$(srcdir)/erasure-code/shec
libec_shec_la_CXXFLAGS= ${AM_CXXFLAGS} \
	-I$(srcdir)/erasure-code/shec
libec_shec_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
#libec_shec_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
#libec_shec_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__erasure_code_.*'
libec_shec_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
libec_shec_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_shec.la
