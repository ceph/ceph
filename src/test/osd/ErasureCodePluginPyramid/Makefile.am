unittest_erasure_code_pyramid_SOURCES = \
	test/osd/ErasureCodePluginPyramid/TestErasureCodePyramid.cc \
	osd/ErasureCode.cc \
	$(libec_pyramid_la_SOURCES)
unittest_erasure_code_pyramid_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_erasure_code_pyramid_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
if LINUX
unittest_erasure_code_pyramid_LDADD += -ldl
endif
check_PROGRAMS += unittest_erasure_code_pyramid

unittest_erasure_code_plugin_pyramid_SOURCES = \
	osd/ErasureCode.cc \
	test/osd/ErasureCodePluginPyramid/TestErasureCodePluginPyramid.cc
unittest_erasure_code_plugin_pyramid_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_erasure_code_plugin_pyramid_LDADD = $(LIBOSD) $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
if LINUX
unittest_erasure_code_plugin_pyramid_LDADD += -ldl
endif
check_PROGRAMS += unittest_erasure_code_plugin_pyramid
