# jerasure plugin
noinst_HEADERS += \
  erasure-code/jerasure/gf-complete/include/gf_complete.h \
  erasure-code/jerasure/gf-complete/include/gf_general.h \
  erasure-code/jerasure/gf-complete/include/gf_int.h \
  erasure-code/jerasure/gf-complete/include/gf_method.h \
  erasure-code/jerasure/gf-complete/include/gf_rand.h \
  erasure-code/jerasure/gf-complete/include/gf_w16.h \
  erasure-code/jerasure/gf-complete/include/gf_w32.h \
  erasure-code/jerasure/gf-complete/include/gf_w4.h \
  erasure-code/jerasure/gf-complete/include/gf_w64.h \
  erasure-code/jerasure/gf-complete/include/gf_w8.h \
  erasure-code/jerasure/jerasure/include/cauchy.h \
  erasure-code/jerasure/jerasure/include/galois.h \
  erasure-code/jerasure/jerasure/include/jerasure.h \
  erasure-code/jerasure/jerasure/include/liberation.h \
  erasure-code/jerasure/jerasure/include/reed_sol.h \
  erasure-code/jerasure/ErasureCodeJerasure.h

jerasure_sources = \
  erasure-code/ErasureCode.cc \
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
  erasure-code/jerasure/gf-complete/src/gf_w8.c \
  erasure-code/jerasure/ErasureCodePluginJerasure.cc \
  erasure-code/jerasure/ErasureCodeJerasure.cc

erasure-code/jerasure/ErasureCodePluginJerasure.cc: ./ceph_ver.h

libec_jerasure_generic_la_SOURCES = ${jerasure_sources}
libec_jerasure_generic_la_CFLAGS = ${AM_CFLAGS}  \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include
libec_jerasure_generic_la_CXXFLAGS= ${AM_CXXFLAGS} \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include
libec_jerasure_generic_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_generic_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_jerasure_generic_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_jerasure_generic.la

libec_jerasure_neon_la_SOURCES = ${jerasure_sources}                                       \
                                  erasure-code/jerasure/gf-complete/src/neon/gf_w4_neon.c  \
                                  erasure-code/jerasure/gf-complete/src/neon/gf_w8_neon.c  \
                                  erasure-code/jerasure/gf-complete/src/neon/gf_w16_neon.c \
                                  erasure-code/jerasure/gf-complete/src/neon/gf_w32_neon.c \
                                  erasure-code/jerasure/gf-complete/src/neon/gf_w64_neon.c
libec_jerasure_neon_la_CFLAGS = ${AM_CFLAGS}  \
	${ARM_NEON_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include
libec_jerasure_neon_la_CXXFLAGS= ${AM_CXXFLAGS} \
	${ARM_NEON_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include
libec_jerasure_neon_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_neon_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_jerasure_neon_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

if HAVE_NEON
erasure_codelib_LTLIBRARIES += libec_jerasure_neon.la
endif

libec_jerasure_sse3_la_SOURCES = ${jerasure_sources}
libec_jerasure_sse3_la_CFLAGS = ${AM_CFLAGS}  \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include
libec_jerasure_sse3_la_CXXFLAGS= ${AM_CXXFLAGS} \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include
libec_jerasure_sse3_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_sse3_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_jerasure_sse3_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

if HAVE_SSSE3
erasure_codelib_LTLIBRARIES += libec_jerasure_sse3.la
endif

libec_jerasure_sse4_la_SOURCES = ${jerasure_sources}
libec_jerasure_sse4_la_CFLAGS = ${AM_CFLAGS}  \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	${INTEL_SSE4_1_FLAGS} \
	${INTEL_SSE4_2_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include
libec_jerasure_sse4_la_CXXFLAGS= ${AM_CXXFLAGS} \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	${INTEL_SSE4_1_FLAGS} \
	${INTEL_SSE4_2_FLAGS} \
	-I$(srcdir)/erasure-code/jerasure/gf-complete/include \
	-I$(srcdir)/erasure-code/jerasure/jerasure/include
libec_jerasure_sse4_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_sse4_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_jerasure_sse4_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

if HAVE_SSE4_PCLMUL
erasure_codelib_LTLIBRARIES += libec_jerasure_sse4.la
endif

libec_jerasure_la_SOURCES = \
	erasure-code/jerasure/ErasureCodePluginSelectJerasure.cc
libec_jerasure_la_CFLAGS = ${AM_CFLAGS}
libec_jerasure_la_CXXFLAGS= ${AM_CXXFLAGS}
libec_jerasure_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_jerasure_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure-code/jerasure/ErasureCodePluginSelectJerasure.cc: ./ceph_ver.h

erasure_codelib_LTLIBRARIES += libec_jerasure.la

EXTRA_DIST += erasure-code/jerasure/CMakeLists.txt
