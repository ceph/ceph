unittest_erasure_code_xor_SOURCES = \
	test/osd/ErasureCodePluginXor/TestErasureCodeXor.cc \
	osd/ErasureCode.cc \
	$(libec_xor_la_SOURCES)
unittest_erasure_code_xor_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_erasure_code_xor_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
if LINUX
unittest_erasure_code_xor_LDADD += -ldl
endif
check_PROGRAMS += unittest_erasure_code_xor

unittest_erasure_code_plugin_xor_SOURCES = \
	osd/ErasureCode.cc \
	test/osd/ErasureCodePluginXor/TestErasureCodePluginXor.cc
unittest_erasure_code_plugin_xor_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_erasure_code_plugin_xor_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
if LINUX
unittest_erasure_code_plugin_xor_LDADD += -ldl
endif
check_PROGRAMS += unittest_erasure_code_plugin_xor
