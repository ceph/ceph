# ISA
noinst_HEADERS += \
	erasure-code/isa/ErasureCodeIsa.h \
	erasure-code/isa/ErasureCodeIsaTableCache.h \
	erasure-code/isa/xor_op.h \
	erasure-code/isa/isa-l/erasure_code/ec_base.h \
	erasure-code/isa/isa-l/include/erasure_code.h \
	erasure-code/isa/isa-l/include/reg_sizes.asm \
	erasure-code/isa/isa-l/include/erasure_code.h \
	erasure-code/isa/isa-l/include/gf_vect_mul.h \
	erasure-code/isa/isa-l/include/types.h

isa_sources = \
	erasure-code/ErasureCode.cc \
	erasure-code/isa/isa-l/erasure_code/ec_base.c \
	erasure-code/isa/isa-l/erasure_code/ec_highlevel_func.c \
	erasure-code/isa/isa-l/erasure_code/ec_multibinary.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_2vect_dot_prod_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_2vect_dot_prod_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_2vect_dot_prod_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_3vect_dot_prod_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_3vect_dot_prod_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_3vect_dot_prod_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_4vect_dot_prod_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_4vect_dot_prod_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_4vect_dot_prod_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_5vect_dot_prod_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_5vect_dot_prod_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_5vect_dot_prod_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_6vect_dot_prod_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_6vect_dot_prod_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_6vect_dot_prod_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_vect_dot_prod_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_vect_dot_prod_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_vect_dot_prod_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_2vect_mad_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_2vect_mad_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_2vect_mad_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_3vect_mad_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_3vect_mad_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_3vect_mad_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_4vect_mad_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_4vect_mad_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_4vect_mad_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_5vect_mad_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_5vect_mad_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_5vect_mad_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_6vect_mad_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_6vect_mad_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_6vect_mad_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_vect_mad_avx2.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_vect_mad_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_vect_mad_sse.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_vect_mul_avx.asm.s \
	erasure-code/isa/isa-l/erasure_code/gf_vect_mul_sse.asm.s \
	erasure-code/isa/ErasureCodeIsa.cc \
	erasure-code/isa/ErasureCodeIsaTableCache.cc \
	erasure-code/isa/ErasureCodePluginIsa.cc \
	erasure-code/isa/xor_op.cc

erasure-code/isa/ErasureCodePluginIsa.cc: ./ceph_ver.h

noinst_LTLIBRARIES += libisa.la
libisa_la_SOURCES = ${isa_sources}
libisa_la_CFLAGS = ${AM_CFLAGS} -I $(srcdir)/erasure-code/isa/isa-l/include/
libisa_la_CXXFLAGS = ${AM_CXXFLAGS}
libisa_la_CCASFLAGS = ${AM_CCASFLAGS} -I $(srcdir)/erasure-code/isa/isa-l/include/
libisa_la_LIBTOOLFLAGS = --tag=CC

libec_isa_la_SOURCES =
libec_isa_la_LIBADD = libisa.la $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_isa_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_isa_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_isa.la

EXTRA_DIST += erasure-code/isa/CMakeLists.txt
