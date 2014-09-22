libosd_types_la_SOURCES = \
	osd/PGLog.cc \
	osd/osd_types.cc \
	osd/ECUtil.cc
libosd_types_la_CXXFLAGS = ${AM_CXXFLAGS}
noinst_LTLIBRARIES += libosd_types.la

if ENABLE_SERVER
libosd_la_SOURCES = \
	osd/PG.cc \
	osd/ReplicatedPG.cc \
	osd/ReplicatedBackend.cc \
	osd/ECBackend.cc \
	osd/ECMsgTypes.cc \
	osd/ECTransaction.cc \
	osd/PGBackend.cc \
	osd/Ager.cc \
	osd/HitSet.cc \
	osd/OSD.cc \
	osd/OSDCap.cc \
	osd/Watch.cc \
	osd/ClassHandler.cc \
	osd/OpRequest.cc \
	common/TrackedOp.cc \
	osd/SnapMapper.cc \
	objclass/class_api.cc
libosd_la_CXXFLAGS = ${AM_CXXFLAGS}
libosd_la_LIBADD = $(LIBOSDC) $(LIBOS) $(LIBOSD_TYPES) $(LIBOS_TYPES)
noinst_LTLIBRARIES += libosd.la
endif # ENABLE_SERVER

noinst_HEADERS += \
	osd/Ager.h \
	osd/ClassHandler.h \
	osd/HitSet.h \
	osd/OSD.h \
	osd/OSDCap.h \
	osd/OSDMap.h \
	osd/ObjectVersioner.h \
	osd/OpRequest.h \
	osd/SnapMapper.h \
	osd/PG.h \
	osd/PGLog.h \
	osd/ReplicatedPG.h \
	osd/PGBackend.h \
	osd/ReplicatedBackend.h \
	osd/TierAgentState.h \
	osd/ECBackend.h \
	osd/ECUtil.h \
	osd/ECMsgTypes.h \
	osd/ECTransaction.h \
	osd/Watch.h \
	osd/osd_types.h

