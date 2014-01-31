unittest_erasure_code_jerasure_SOURCES = \
	test/osd/ErasureCodePluginJerasure/TestErasureCodeJerasure.cc \
	osd/ErasureCode.cc \
	$(libec_jerasure_la_SOURCES)
unittest_erasure_code_jerasure_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_erasure_code_jerasure_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
if LINUX
unittest_erasure_code_jerasure_LDADD += -ldl
endif
check_PROGRAMS += unittest_erasure_code_jerasure

unittest_erasure_code_plugin_jerasure_SOURCES = \
	osd/ErasureCode.cc \
	test/osd/ErasureCodePluginJerasure/TestErasureCodePluginJerasure.cc
unittest_erasure_code_plugin_jerasure_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_erasure_code_plugin_jerasure_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
if LINUX
unittest_erasure_code_plugin_jerasure_LDADD += -ldl
endif
check_PROGRAMS += unittest_erasure_code_plugin_jerasure
