## erasure code plugins
erasure_codelibdir = $(libdir)/erasure-code
erasure_codelib_LTLIBRARIES =  

include osd/ErasureCodePluginJerasure/Makefile.am

libosd_la_SOURCES = \
	osd/ErasureCodePlugin.cc \
	osd/PG.cc \
	osd/PGLog.cc \
	osd/ReplicatedPG.cc \
	osd/Ager.cc \
	osd/OSD.cc \
	osd/OSDCap.cc \
	osd/Watch.cc \
	osd/ClassHandler.cc \
	osd/OpRequest.cc \
	osd/SnapMapper.cc \
	osd/osd_types.cc \
	objclass/class_api.cc
libosd_la_LIBADD = $(LIBOSDC) $(LIBOS)
noinst_LTLIBRARIES += libosd.la

noinst_HEADERS += \
	osd/Ager.h \
	osd/ClassHandler.h \
	osd/ErasureCodeInterface.h \
	osd/ErasureCodePlugin.h \
	osd/OSD.h \
	osd/OSDCap.h \
	osd/OSDMap.h \
	osd/ObjectVersioner.h \
	osd/OpRequest.h \
	osd/SnapMapper.h \
	osd/PG.h \
	osd/PGLog.h \
	osd/ReplicatedPG.h \
	osd/Watch.h \
	osd/osd_types.h

