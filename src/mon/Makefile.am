libmon_types_la_SOURCES = \
	mon/PGMap.cc
noinst_LTLIBRARIES += libmon_types.la

if ENABLE_SERVER
if WITH_MON

libmon_a_SOURCES = \
	mon/Monitor.cc \
	mon/Paxos.cc \
	mon/PaxosService.cc \
	mon/OSDMonitor.cc \
	mon/MDSMonitor.cc \
	mon/MonmapMonitor.cc \
	mon/PGMonitor.cc \
	mon/LogMonitor.cc \
	mon/AuthMonitor.cc \
	mon/Elector.cc \
	mon/HealthMonitor.cc \
	mon/DataHealthService.cc \
	mon/ConfigKeyService.cc
libmon_a_LIBADD =
noinst_LIBRARIES += libmon.a

noinst_HEADERS += \
	mon/AuthMonitor.h \
	mon/DataHealthService.h \
	mon/Elector.h \
	mon/LogMonitor.h \
	mon/ConfigKeyService.h \
	mon/HealthMonitor.h \
	mon/HealthService.h \
	mon/MDSMonitor.h \
	mon/MonmapMonitor.h \
	mon/MonCap.h \
	mon/MonClient.h \
	mon/MonCommands.h \
	mon/DumplingMonCommands.h \
	mon/MonMap.h \
	mon/Monitor.h \
	mon/MonitorDBStore.h \
	mon/MonOpRequest.h \
	mon/OSDMonitor.h \
	mon/PGMap.h \
	mon/PGMonitor.h \
	mon/Paxos.h \
	mon/PaxosService.h \
	mon/QuorumService.h \
	mon/Session.h \
	mon/mon_types.h

endif # WITH_MON
endif # ENABLE_SERVER
