# jerasure plugin
noinst_HEADERS += \
  erasure-code/jerasure/jerasure/include/cauchy.h \
  erasure-code/jerasure/jerasure/include/galois.h \
  erasure-code/jerasure/jerasure/include/jerasure.h \
  erasure-code/jerasure/jerasure/include/liberation.h \
  erasure-code/jerasure/jerasure/include/reed_sol.h \
  erasure-code/jerasure/gf-complete/include/gf_int.h \
  erasure-code/jerasure/gf-complete/include/gf_complete.h \
  erasure-code/jerasure/gf-complete/include/gf_rand.h \
  erasure-code/jerasure/gf-complete/include/gf_method.h \
  erasure-code/jerasure/gf-complete/include/gf_general.h \
  erasure-code/jerasure/ErasureCodeJerasure.h

if ENABLE_SERVER
jerasure_sources = \
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

libec_jerasure_generic_la_SOURCES = ${jerasure_sources}
libec_jerasure_generic_la_CFLAGS = ${AM_CFLAGS}  \
	-Ierasure-code/jerasure/gf-complete/include \
	-Ierasure-code/jerasure/jerasure/include
libec_jerasure_generic_la_CXXFLAGS= ${AM_CXXFLAGS} \
	-Ierasure-code/jerasure/gf-complete/include \
	-Ierasure-code/jerasure/jerasure/include
libec_jerasure_generic_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_generic_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
if LINUX
libec_jerasure_generic_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_jerasure_generic.la

libec_jerasure_sse3_la_SOURCES = ${jerasure_sources}
libec_jerasure_sse3_la_CFLAGS = ${AM_CFLAGS}  \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	-Ierasure-code/jerasure/gf-complete/include \
	-Ierasure-code/jerasure/jerasure/include
libec_jerasure_sse3_la_CXXFLAGS= ${AM_CXXFLAGS} \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	-Ierasure-code/jerasure/gf-complete/include \
	-Ierasure-code/jerasure/jerasure/include
libec_jerasure_sse3_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_sse3_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
if LINUX
libec_jerasure_sse3_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_jerasure_sse3.la

libec_jerasure_sse4_la_SOURCES = ${jerasure_sources}
libec_jerasure_sse4_la_CFLAGS = ${AM_CFLAGS}  \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	${INTEL_SSE4_1_FLAGS} \
	${INTEL_SSE4_2_FLAGS} \
	-Ierasure-code/jerasure/gf-complete/include \
	-Ierasure-code/jerasure/jerasure/include
libec_jerasure_sse4_la_CXXFLAGS= ${AM_CXXFLAGS} \
	${INTEL_SSE_FLAGS} \
	${INTEL_SSE2_FLAGS} \
	${INTEL_SSE3_FLAGS} \
	${INTEL_SSSE3_FLAGS} \
	${INTEL_SSE4_1_FLAGS} \
	${INTEL_SSE4_2_FLAGS} \
	-Ierasure-code/jerasure/gf-complete/include \
	-Ierasure-code/jerasure/jerasure/include
libec_jerasure_sse4_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_sse4_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
if LINUX
libec_jerasure_sse4_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_jerasure_sse4.la

libec_jerasure_la_SOURCES = \
	erasure-code/jerasure/ErasureCodePluginSelectJerasure.cc
libec_jerasure_la_CFLAGS = ${AM_CFLAGS} 
libec_jerasure_la_CXXFLAGS= ${AM_CXXFLAGS}
libec_jerasure_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_jerasure_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
if LINUX
libec_jerasure_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_jerasure.la
endif # ENABLE_SERVER
