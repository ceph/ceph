#
# note: the old Makefile is at Makefile.old
#

AUTOMAKE_OPTIONS = gnu

# monitor
cmon_SOURCES = cmon.cc msg/SimpleMessenger.cc
cmon_LDADD = libmon.a libcrush.a libcommon.a
mkmonmap_SOURCES = mkmonmap.cc
mkmonmap_LDADD = libcommon.a
monmaptool_SOURCES = monmaptool.cc
monmaptool_LDADD = libcommon.a
cmonctl_SOURCES = cmonctl.cc msg/SimpleMessenger.cc
cmonctl_LDADD = libcommon.a
crushtool_SOURCES = crushtool.cc
crushtool_LDADD = libcommon.a libcrush.a

# mds
cmds_SOURCES = cmds.cc msg/SimpleMessenger.cc
cmds_LDADD = libmds.a libosdc.a libcrush.a libcommon.a

# osd 
cosd_SOURCES = cosd.cc msg/SimpleMessenger.cc
cosd_LDADD = libosd.a libebofs.a libcrush.a libcommon.a

# synthetic client
csyn_SOURCES = csyn.cc msg/SimpleMessenger.cc
csyn_LDADD = libclient.a libosdc.a libcrush.a libcommon.a

# fuse client
if WITH_FUSE
FUSEBIN = cfuse fakefuse
else
FUSEBIN = 
endif

cfuse_SOURCES = cfuse.cc msg/SimpleMessenger.cc client/fuse.cc client/fuse_ll.cc
cfuse_LDADD = -lfuse libclient.a libosdc.a libcrush.a libcommon.a
fakefuse_SOURCES = fakefuse.cc msg/FakeMessenger.cc client/fuse.cc client/fuse_ll.cc
fakefuse_LDADD = -lfuse libmon.a libmds.a libosd.a libebofs.a \
	libclient.a libosdc.a libcrush.a libcommon.a

# debug targets
fakesyn_SOURCES = fakesyn.cc msg/FakeMessenger.cc
fakesyn_LDADD = libmon.a libmds.a libosd.a libebofs.a \
	libclient.a libosdc.a libcrush.a libcommon.a

#newsyn_SOURCES = newsyn.cc msg/SimpleMessenger.cc
#newsyn_LDADD = libmon.a libmds.a libosd.a libebofs.a \
#	libclient.a libosdc.a libcrush.a libcommon.a

# tester tools
streamtest_SOURCES = streamtest.cc
streamtest_LDADD = libosd.a libebofs.a libcommon.a

##
INCLUDES = 
LDADD = -lpthread 

AM_CXXFLAGS = -Wall -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE
AM_CFLAGS = -Wall -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE
AM_LDFLAGS =

bin_PROGRAMS = \
	cmon cmds cosd csyn \
	mkmonmap monmaptool crushtool cmonctl \
	fakesyn \
	streamtest \
	$(FUSEBIN) $(NEWSYN)
noinst_LIBRARIES = \
	libcommon.a libcrush.a \
	libmon.a libmds.a libosdc.a libosd.a libebofs.a libclient.a

# extra bits
EXTRA_DIST = start.sh stop.sh

# libs
libcommon_a_SOURCES = \
	msg/Message.cc \
	common/Logger.cc \
	common/Clock.cc \
	common/Timer.cc \
	common/Finisher.cc \
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
	osd/FileJournal.cc

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
	osd/JournalingObjectStore.cc \
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

# headers... yuck
noinst_HEADERS = \
	msg/tcp.cc\
	client/Trace.h\
	client/hadoop/CephFSInterface.h\
	client/SyntheticClient.h\
	client/fuse.h\
	client/fuse_ll.h\
	client/FileCache.h\
	client/Client.h\
	common/Clock.h\
	common/Cond.h\
	common/DecayCounter.h\
	common/LogType.h\
	common/Logger.h\
	common/Mutex.h\
	common/RWLock.h\
	common/Semaphore.h\
	common/ThreadPool.h\
	common/Timer.h\
	common/Thread.h\
	common/Finisher.h\
	crush/builder.h\
	crush/hash.h\
	crush/mapper.h\
	crush/types.h\
	crush/CrushWrapper.h\
	crush/crush.h\
	ebofs/Table.h\
	ebofs/csum.h\
	ebofs/BlockDevice.h\
	ebofs/Ebofs.h\
	osd/FileJournal.h\
	osd/JournalingObjectStore.h\
	ebofs/types.h\
	ebofs/Allocator.h\
	ebofs/BufferCache.h\
	osd/Journal.h\
	ebofs/nodes.h\
	ebofs/Cnode.h\
	ebofs/Onode.h\
	include/Context.h\
	include/Distribution.h\
	include/bitmapper.h\
	include/blobhash.h\
	include/error.h\
	include/filepath.h\
	include/frag.h\
	include/hash.h\
	include/interval_set.h\
	include/lru.h\
	include/pobject.h\
	include/rangeset.h\
	include/statlite.h\
	include/triple.h\
	include/uofs.h\
	include/encodable.h\
	include/byteorder.h\
	include/inttypes.h\
	include/utime.h\
	include/object.h\
	include/ceph_fs.h\
	include/atomic.h\
	include/buffer.h\
	include/page.h\
	include/xlist.h\
	include/types.h\
	kernel/crush/crush.h\
	kernel/crush/hash.h\
	kernel/crush/mapper.h\
	kernel/osdmap.h\
	kernel/mdsmap.h\
	kernel/super.h\
	kernel/mon_client.h\
	kernel/messenger.h\
	kernel/ktcp.h\
	kernel/mds_client.h\
	kernel/osd_client.h\
	mds/Anchor.h\
	mds/AnchorClient.h\
	mds/AnchorTable.h\
	mds/CDentry.h\
	mds/CDir.h\
	mds/IdAllocator.h\
	mds/LocalLock.h\
	mds/LogEvent.h\
	mds/MDBalancer.h\
	mds/MDLog.h\
	mds/Migrator.h\
	mds/ScatterLock.h\
	mds/SimpleLock.h\
	mds/events/EAnchor.h\
	mds/events/EAnchorClient.h\
	mds/events/ESessions.h\
	mds/events/EUpdate.h\
	mds/events/EExport.h\
	mds/events/EFragment.h\
	mds/events/EImportFinish.h\
	mds/events/EImportStart.h\
	mds/events/EMetaBlob.h\
	mds/events/EPurgeFinish.h\
	mds/events/ESession.h\
	mds/events/ESlaveUpdate.h\
	mds/events/EString.h\
	mds/events/EOpen.h\
	mds/events/ESubtreeMap.h\
	mds/Locker.h\
	mds/LogSegment.h\
	mds/CInode.h\
	mds/FileLock.h\
	mds/SessionMap.h\
	mds/Capability.h\
	mds/mdstypes.h\
	mds/MDCache.h\
	mds/Server.h\
	mds/MDSMap.h\
	mds/MDS.h\
	messages/MExportCapsAck.h\
	messages/MExportDirAck.h\
	messages/MExportDirCancel.h\
	messages/MExportDirDiscover.h\
	messages/MExportDirDiscoverAck.h\
	messages/MExportDirFinish.h\
	messages/MExportDirNotify.h\
	messages/MExportDirNotifyAck.h\
	messages/MExportDirPrep.h\
	messages/MExportDirPrepAck.h\
	messages/MExportDirWarning.h\
	messages/MExportDirWarningAck.h\
	messages/MGenericMessage.h\
	messages/MInodeFileCaps.h\
	messages/MLock.h\
	messages/MMDSBeacon.h\
	messages/MMDSCacheRejoin.h\
	messages/MMDSFragmentNotify.h\
	messages/MMDSMap.h\
	messages/MMDSResolve.h\
	messages/MMDSResolveAck.h\
	messages/MMDSSlaveRequest.h\
	messages/MMonCommand.h\
	messages/MClientReconnect.h\
	messages/MClientUnmount.h\
	messages/MOSDGetMap.h\
	messages/MOSDSubOp.h\
	messages/MOSDIn.h\
	messages/MMDSBoot.h\
	messages/MClientMount.h\
	messages/MClientFileCaps.h\
	messages/MOSDMap.h\
	messages/MOSDOp.h\
	messages/MOSDOut.h\
	messages/MOSDPGActivateSet.h\
	messages/MOSDPGNotify.h\
	messages/MOSDPGQuery.h\
	messages/MOSDPGRemove.h\
	messages/MOSDPing.h\
	messages/MClientRequestForward.h\
	messages/MDentryUnlink.h\
	messages/MDirUpdate.h\
	messages/MClientRequest.h\
	messages/MOSDPGLog.h\
	messages/MPGStats.h\
	messages/MPing.h\
	messages/MOSDPGPeer.h\
	messages/MOSDPGPeerAck.h\
	messages/MOSDPGPeerRequest.h\
	messages/MHeartbeat.h\
	messages/MOSDPGSummary.h\
	messages/MOSDPGUpdate.h\
	messages/MExportCaps.h\
	messages/MMDSGetMap.h\
	messages/MMonCommandAck.h\
	messages/MMonElection.h\
	messages/MOSDOpReply.h\
	messages/MMonMap.h\
	messages/MStatfs.h\
	messages/MOSDSubOpReply.h\
	messages/MPingAck.h\
	messages/MClientReply.h\
	messages/MDiscover.h\
	messages/MClientSession.h\
	messages/MMonPaxos.h\
	messages/MOSDBoot.h\
	messages/MStatfsReply.h\
	messages/MAnchor.h\
	messages/MCacheExpire.h\
	messages/MDiscoverReply.h\
	messages/MOSDFailure.h\
	messages/MExportDir.h\
	mon/Elector.h\
	mon/MonitorStore.h\
	mon/Paxos.h\
	mon/PaxosService.h\
	mon/mon_types.h\
	mon/MDSMonitor.h\
	mon/Monitor.h\
	mon/PGMonitor.h\
	mon/ClientMonitor.h\
	mon/OSDMonitor.h\
	mon/MonMap.h\
	mon/PGMap.h\
	msg/FakeMessenger.h\
	msg/tcp.h\
	msg/Message.h\
	msg/msg_types.h\
	msg/Dispatcher.h\
	msg/Messenger.h\
	msg/SimpleMessenger.h\
	osbdb/OSBDB.h\
	osd/Ager.h\
	osd/BDBMap.h\
	osd/Fake.h\
	osd/FakeStoreBDBCollections.h\
	osd/ObjectStore.h\
	osd/ObjectVersioner.h\
	osd/RAID4PG.h\
	osd/ReplicatedPG.h\
	osd/PG.h\
	osd/OSDMap.h\
	osd/FakeStore.h\
	osd/OSD.h\
	osd/osd_types.h\
	osdc/Blinker.h\
	osdc/Journaler.h\
	osdc/Filer.h\
	osdc/ObjectCacher.h\
	osdc/Objecter.h\
	config.h\
	include/intarith.h\
	include/err.h


