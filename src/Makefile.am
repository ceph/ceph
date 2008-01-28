AUTOMAKE_OPTIONS = gnu

INCLUDES = 
LDADD = -lpthread libcommon.a

bin_PROGRAMS = cmon mkmonmap cmonctl cmds cosd

# monitor
cmon_SOURCES = cmon.cc msg/SimpleMessenger.cc
cmon_LDADD = libmon.a libcommon.a libcrush.a

mkmonmap_SOURCES = mkmonmap.cc
mkmonmap_LDADD = libcommon.a

cmonctl_SOURCES = cmonctl.cc msg/SimpleMessenger.cc
cmonctl_LDADD = libcommon.a

# mds
cmds_SOURCES = cmds.cc msg/SimpleMessenger.cc
cmds_LDADD = libmds.a libosdc.a libcommon.a libcrush.a

# osd 
cosd_SOURCES = cosd.cc msg/SimpleMessenger.cc
cosd_LDADD = libosd.a libebofs.a libcommon.a libcrush.a


noinst_LIBRARIES = \
	libcommon.a libcrush.a \
	libmon.a libmds.a libosdc.a libosd.a libebofs.a libclient.a

libcommon_a_SOURCES = \
	msg/Message.cc \
	common/Logger.cc \
	common/Clock.cc \
	common/Timer.cc \
	mon/MonMap.cc \
	config.cc

libcrush_a_SOURCES = \
	crush/builder.c \
	crush/mapper.c \
	crush/crush.c

libmon_a_SOURCES = \
	mon/Monitor.cc \
	mon/Paxos.cc \
	mon/PaxosService.cc \
	mon/OSDMonitor.cc \
	mon/MDSMonitor.cc \
	mon/ClientMonitor.cc \
	mon/PGMonitor.cc \
	mon/Elector.cc \
	mon/MonitorStore.cc

libebofs_a_SOURCES = \
	ebofs/BlockDevice.cc \
	ebofs/BufferCache.cc \
	ebofs/Ebofs.cc \
	ebofs/Allocator.cc \
	ebofs/FileJournal.cc

libmds_a_SOURCES = \
	mds/MDS.cc \
	mds/journal.cc \
	mds/Server.cc \
	mds/MDCache.cc \
	mds/Locker.cc \
	mds/Migrator.cc \
	mds/MDBalancer.cc \
	mds/CDentry.cc \
	mds/CDir.cc \
	mds/CInode.cc \
	mds/AnchorTable.cc \
	mds/AnchorClient.cc \
	mds/LogEvent.cc \
	mds/IdAllocator.cc \
	mds/SessionMap.cc \
	mds/MDLog.cc

libosd_a_SOURCES = \
	osd/PG.cc \
	osd/ReplicatedPG.cc \
	osd/RAID4PG.cc \
	osd/Ager.cc \
	osd/FakeStore.cc \
	osd/OSD.cc

libosdc_a_SOURCES = \
	osdc/Objecter.cc \
	osdc/ObjectCacher.cc \
	osdc/Filer.cc \
	osdc/Journaler.cc

libclient_a_SOURCES = \
	client/FileCache.cc \
	client/Client.cc \
	client/SyntheticClient.cc \
	client/Trace.cc
