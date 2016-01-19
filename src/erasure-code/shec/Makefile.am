# SHEC plugin
shec_sources = \
	erasure-code/ErasureCode.cc \
	erasure-code/shec/ErasureCodePluginShec.cc \
	erasure-code/shec/ErasureCodeShec.cc \
	erasure-code/shec/ErasureCodeShecTableCache.cc \
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

libec_shec_generic_la_SOURCES = ${shec_sources}
libec_shec_generic_la_CFLAGS = ${AM_CFLAGS}  \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure \
	-I$(srcdir)/erasure-code/shec
libec_shec_generic_la_CXXFLAGS= ${AM_CXXFLAGS} \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure \
	-I$(srcdir)/erasure-code/shec
libec_shec_generic_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_shec_generic_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_shec_generic_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_shec_generic.la

libec_shec_neon_la_SOURCES = ${shec_sources} \
	erasure-code/jerasure/gf-complete/src/neon/gf_w4_neon.c \
	erasure-code/jerasure/gf-complete/src/neon/gf_w8_neon.c \
	erasure-code/jerasure/gf-complete/src/neon/gf_w16_neon.c \
	erasure-code/jerasure/gf-complete/src/neon/gf_w32_neon.c \
	erasure-code/jerasure/gf-complete/src/neon/gf_w64_neon.c
libec_shec_neon_la_CFLAGS = ${AM_CFLAGS}  \
	${ARM_NEON_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure \
	-I$(srcdir)/erasure-code/shec
libec_shec_neon_la_CXXFLAGS= ${AM_CXXFLAGS} \
	${ARM_NEON_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure \
	-I$(srcdir)/erasure-code/shec
libec_shec_neon_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_shec_neon_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_shec_neon_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

if HAVE_NEON
erasure_codelib_LTLIBRARIES += libec_shec_neon.la
endif

libec_shec_sse3_la_SOURCES = ${shec_sources}
libec_shec_sse3_la_CFLAGS = ${AM_CFLAGS}  \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure \
	-I$(srcdir)/erasure-code/shec
libec_shec_sse3_la_CXXFLAGS= ${AM_CXXFLAGS} \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure \
	-I$(srcdir)/erasure-code/shec
libec_shec_sse3_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_shec_sse3_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_shec_sse3_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

if HAVE_SSSE3
erasure_codelib_LTLIBRARIES += libec_shec_sse3.la
endif

libec_shec_sse4_la_SOURCES = ${shec_sources}
libec_shec_sse4_la_CFLAGS = ${AM_CFLAGS}  \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	${INTEL_SSE4_1_FLAGS} \
	${INTEL_SSE4_2_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure \
	-I$(srcdir)/erasure-code/shec
libec_shec_sse4_la_CXXFLAGS= ${AM_CXXFLAGS} \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	${INTEL_SSE4_1_FLAGS} \
	${INTEL_SSE4_2_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure \
	-I$(srcdir)/erasure-code/shec
libec_shec_sse4_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_shec_sse4_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_shec_sse4_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

if HAVE_SSE4_PCLMUL
erasure_codelib_LTLIBRARIES += libec_shec_sse4.la
endif

libec_shec_la_SOURCES = \
	erasure-code/shec/ErasureCodePluginSelectShec.cc
libec_shec_la_CFLAGS = ${AM_CFLAGS}
libec_shec_la_CXXFLAGS= ${AM_CXXFLAGS}
libec_shec_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_shec_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_shec_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure-code/shec/ErasureCodePluginSelectShec.cc: ./ceph_ver.h

erasure_codelib_LTLIBRARIES += libec_shec.la
