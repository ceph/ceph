# SHEC plugin

libec_shec_la_SOURCES = \
	erasure-code/ErasureCode.cc \
	erasure-code/shec/ErasureCodePluginShec.cc \
	erasure-code/shec/ErasureCodeShec.cc \
	erasure-code/shec/ErasureCodeShecTableCache.cc \
	erasure-code/shec/shec.cc \
	erasure-code/shec/determinant.c \
	erasure-code/jerasure/jerasure/src/cauchy.c \
	erasure-code/jerasure/jerasure/src/galois.c \
	erasure-code/jerasure/jerasure/src/jerasure.c \
	erasure-code/jerasure/jerasure/src/liberation.c \
	erasure-code/jerasure/jerasure/src/reed_sol.c \
	erasure-code/jerasure/gf-complete/src/gf_wgen.c \
	erasure-code/jerasure/gf-complete/src/gf_method.c \
	erasure-code/jerasure/gf-complete/src/gf_w16.c \
	erasure-code/jerasure/gf-complete/src/gf.c \
	erasure-code/jerasure/gf-complete/src/gf_w32.c \
	erasure-code/jerasure/gf-complete/src/gf_w64.c \
	erasure-code/jerasure/gf-complete/src/gf_w128.c \
	erasure-code/jerasure/gf-complete/src/gf_general.c \
	erasure-code/jerasure/gf-complete/src/gf_w4.c \
	erasure-code/jerasure/gf-complete/src/gf_rand.c \
	erasure-code/jerasure/gf-complete/src/gf_w8.c
noinst_HEADERS += \
	erasure-code/shec/ErasureCodeShec.h \
	erasure-code/shec/ErasureCodeShecTableCache.h \
	erasure-code/shec/shec.h \
	erasure-code/jerasure/jerasure/include/cauchy.h \
	erasure-code/jerasure/jerasure/include/galois.h \
	erasure-code/jerasure/jerasure/include/jerasure.h \
	erasure-code/jerasure/jerasure/include/liberation.h \
	erasure-code/jerasure/jerasure/include/reed_sol.h \
	erasure-code/jerasure/gf-complete/include/gf_int.h \
	erasure-code/jerasure/gf-complete/include/gf_complete.h \
	erasure-code/jerasure/gf-complete/include/gf_rand.h \
	erasure-code/jerasure/gf-complete/include/gf_method.h \
	erasure-code/jerasure/gf-complete/include/gf_general.h

erasure-code/shec/ErasureCodePluginShec.cc: ./ceph_ver.h

libec_shec_la_CFLAGS = ${AM_CFLAGS} \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure \
	-I$(srcdir)/erasure-code/shec
libec_shec_la_CXXFLAGS= ${AM_CXXFLAGS} \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure \
	-I$(srcdir)/erasure-code/shec
libec_shec_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
#libec_shec_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
#libec_shec_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__erasure_code_.*'
libec_shec_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
libec_shec_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_shec.la
