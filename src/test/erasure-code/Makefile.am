ceph_erasure_code_benchmark_SOURCES = \
	test/erasure-code/ceph_erasure_code_benchmark.cc
ceph_erasure_code_benchmark_LDADD = $(LIBOSD) $(LIBCOMMON) $(BOOST_PROGRAM_OPTIONS_LIBS) $(CEPH_GLOBAL)
if LINUX
ceph_erasure_code_benchmark_LDADD += -ldl
endif
bin_DEBUGPROGRAMS += ceph_erasure_code_benchmark

noinst_HEADERS += \
	test/erasure-code/ceph_erasure_code_benchmark.h
