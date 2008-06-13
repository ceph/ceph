#
# note: the old Makefile is at Makefile.old
#

AUTOMAKE_OPTIONS = gnu

# monitor
cmon_SOURCES = cmon.cc msg/SimpleMessenger.cc
cmon_LDADD = libmon.a libcrush.a libcommon.a
cmonctl_SOURCES = cmonctl.cc msg/SimpleMessenger.cc
cmonctl_LDADD = libcommon.a

# tools
mkmonfs_SOURCES = mkmonfs.cc
mkmonfs_LDADD = libmon.a libcommon.a libcrush.a
monmaptool_SOURCES = monmaptool.cc
monmaptool_LDADD = libcommon.a
crushtool_SOURCES = crushtool.cc
crushtool_LDADD = libcommon.a libcrush.a
osdmaptool_SOURCES = osdmaptool.cc
osdmaptool_LDADD = libmon.a libcommon.a libcrush.a

psim_SOURCES = psim.cc
psim_LDADD = libcommon.a libcrush.a

test_ebofs_SOURCES = ebofs/test.ebofs.cc
test_ebofs_LDADD = libebofs.a libcommon.a libos.a
mkfs_ebofs_SOURCES = ebofs/mkfs.ebofs.cc
mkfs_ebofs_LDADD = libebofs.a libcommon.a libos.a

# mds
cmds_SOURCES = cmds.cc msg/SimpleMessenger.cc
cmds_LDADD = libmds.a libosdc.a libcrush.a libcommon.a

dumpjournal_SOURCES = dumpjournal.cc msg/SimpleMessenger.cc
dumpjournal_LDADD = libosdc.a libcrush.a libcommon.a

# osd 
cosd_SOURCES = cosd.cc msg/SimpleMessenger.cc
cosd_LDADD = libosd.a libos.a libebofs.a libcrush.a libcommon.a
dupstore_SOURCES = dupstore.cc config.cc
dupstore_LDADD = libos.a libebofs.a libcommon.a
streamtest_SOURCES = streamtest.cc
streamtest_LDADD = libos.a libebofs.a libcommon.a

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
fakefuse_LDADD = -lfuse libmon.a libmds.a libosd.a libos.a libebofs.a \
	libclient.a libosdc.a libcrush.a libcommon.a

# debug targets
fakesyn_SOURCES = fakesyn.cc msg/FakeMessenger.cc
fakesyn_LDADD = libmon.a libmds.a libosd.a libos.a libebofs.a \
	libclient.a libosdc.a libcrush.a libcommon.a

#newsyn_SOURCES = newsyn.cc msg/SimpleMessenger.cc
#newsyn_LDADD = libmon.a libmds.a libosd.a libebofs.a \
#	libclient.a libosdc.a libcrush.a libcommon.a

## libcrush.so
libcrush_so_a_SOURCES = \
	crush/builder.c \
	crush/mapper.c \
	crush/crush.c
libcrush_so_a_CFLAGS = ${AM_CFLAGS} -fPIC

#libcrush.so: libcrush_so.a
#	${CC} -I. -fPIC -shared -Wl,-soname,$@.1 ${CFLAGS} ${LIBS} ${libcrush_so_a_SOURCES} -o $@
#BUILT_SOURCES = libcrush.so

# crushwrapper
SWIG_FILES = crush/CrushWrapper.i crush/CrushWrapper.h
crush/CrushWrapper_wrap.cxx: ${SWIG_FILES}
	${SWIG} -perl5 -c++ -shadow -outdir . $<

libCrushWrapper.so: crush/CrushWrapper_wrap.cxx libcrush_so.a
	${CXX} ${CXXFLAGS} ${SWIG_PL_INCLUDES} -I. -shared -fPIC $^ config.cc -o $@

#BUILT_SOURCES = libCrushWrapper.so


## libcephclient.so
libcephclient_so_a_SOURCES = \
	client/Client.cc \
	client/SyntheticClient.cc \
	client/Trace.cc \
	msg/Message.cc \
	common/Logger.cc \
	common/Clock.cc \
	common/Timer.cc \
	common/Finisher.cc \
	mon/MonMap.cc \
	mon/MonClient.cc \
	osd/OSDMap.cc \
	config.cc \
	osdc/Objecter.cc \
	osdc/ObjectCacher.cc \
	osdc/Filer.cc \
	osdc/Journaler.cc
libcephclient_so_a_CXXFLAGS = ${AM_CXXFLAGS} -fPIC
libcephclient_so_a_CFLAGS = ${AM_CFLAGS} -fPIC

#BUILT_SOURCES += libcephclient_so.a

#libcephclient.so: libcephclient_so.a libcrush_so.a
#	${CXX} -I. -fPIC -shared -Wl,-soname,$@.1 ${AM_CXXFLAGS} ${LIBS} $^ -o $@
#BUILT_SOURCES += libcephclient.so

## hadoop client
JAVA_BASE = /usr/lib/jvm/java-6-sun
libhadoopcephfs.so: client/hadoop/CephFSInterface.cc libcephclient_so.a
	${CXX} -fPIC -shared -Wl,-soname,$@.1 -I. ${AM_CXXFLAGS} -I${JAVA_BASE}/include -I${JAVA_BASE}/include/linux ${LIBS} $^ -o $@

#BUILT_SOURCES += libhadoopcephfs.so




##
INCLUDES = 
LDADD = -lpthread 

AM_CXXFLAGS = -Wall -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE
AM_CFLAGS = -Wall -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE
AM_LDFLAGS =

bin_PROGRAMS = \
	cmon cmds cosd csyn \
	cmonctl \
	mkmonfs monmaptool osdmaptool crushtool \
	fakesyn \
	streamtest dupstore psim dumpjournal \
	test.ebofs mkfs.ebofs \
	$(FUSEBIN) $(NEWSYN)
noinst_LIBRARIES = \
	libcommon.a libcrush.a \
	libmon.a libmds.a libosdc.a libosd.a libclient.a \
	libos.a libebofs.a 

noinst_LIBRARIES += libcrush_so.a #libcephclient_so.a

# extra bits
EXTRA_DIST = mkcephfs.sh mkfs.sh restart.sh startnew.sh stop.sh

# cleaning
clean-local:
	-rm crush/*.cxx
	-rm *.so
	-rm CrushWrapper.pm

# libs
libcommon_a_SOURCES = \
	msg/Message.cc \
	common/Logger.cc \
	common/Clock.cc \
	common/Timer.cc \
	common/Finisher.cc \
	mon/MonMap.cc \
	mon/MonClient.cc \
	osd/OSDMap.cc \
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

libmds_a_SOURCES = \
	mds/MDS.cc \
	mds/journal.cc \
	mds/Server.cc \
	mds/MDCache.cc \
	mds/CacheObject.cc \
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

libebofs_a_SOURCES = \
	ebofs/BlockDevice.cc \
	ebofs/BufferCache.cc \
	ebofs/Ebofs.cc \
	ebofs/Allocator.cc
libos_a_SOURCES = \
	os/FileJournal.cc \
	os/FileStore.cc \
	os/JournalingObjectStore.cc

libosd_a_SOURCES = \
	osd/PG.cc \
	osd/ReplicatedPG.cc \
	osd/RAID4PG.cc \
	osd/Ager.cc \
	osd/OSD.cc

libosdc_a_SOURCES = \
	osdc/Objecter.cc \
	osdc/ObjectCacher.cc \
	osdc/Filer.cc \
	osdc/Journaler.cc

libclient_a_SOURCES = \
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
	crush/CrushWrapper.i\
	crush/crush.h\
	crush/grammar.h\
	crush/sample.txt\
	ebofs/Table.h\
	ebofs/csum.h\
	ebofs/BlockDevice.h\
	ebofs/Ebofs.h\
	os/FileJournal.h\
	os/JournalingObjectStore.h\
	ebofs/types.h\
	ebofs/Allocator.h\
	ebofs/BufferCache.h\
	os/Journal.h\
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
	kernel/crush/crush.c\
	kernel/crush/crush.h\
	kernel/crush/mapper.c\
	kernel/crush/mapper.h\
	kernel/crush/hash.h\
	kernel/Makefile\
	kernel/addr.c\
	kernel/decode.h\
	kernel/dir.c\
	kernel/export.c\
	kernel/file.c\
	kernel/inode.c\
	kernel/ktcp.c\
	kernel/ktcp.h\
	kernel/mds_client.c\
	kernel/mds_client.h\
	kernel/mdsmap.c\
	kernel/mdsmap.h\
	kernel/messenger.c\
	kernel/messenger.h\
	kernel/mon_client.c\
	kernel/mon_client.h\
	kernel/osd_client.c\
	kernel/osd_client.h\
	kernel/osdmap.c\
	kernel/osdmap.h\
	kernel/proc.c\
	kernel/super.c\
	kernel/super.h\
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
	messages/MClientLease.h\
	messages/MOSDMap.h\
	messages/MOSDOp.h\
	messages/MOSDOut.h\
	messages/MOSDPGInfo.h\
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
	messages/MMonGetMap.h\
	messages/MStatfs.h\
	messages/MOSDSubOpReply.h\
	messages/MPingAck.h\
	messages/MClientReply.h\
	messages/MDiscover.h\
	messages/MClientSession.h\
	messages/MMonPaxos.h\
	messages/MOSDBoot.h\
	messages/MOSDPGCreate.h\
	messages/MStatfsReply.h\
	messages/MAnchor.h\
	messages/MCacheExpire.h\
	messages/MDiscoverReply.h\
	messages/MOSDFailure.h\
	messages/MExportDir.h\
	messages/MOSDAlive.h\
	messages/MPGStatsAck.h\
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
	mon/MonClient.h\
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
	os/BDBMap.h\
	os/Fake.h\
	os/FakeStoreBDBCollections.h\
	os/ObjectStore.h\
	osd/ObjectVersioner.h\
	osd/RAID4PG.h\
	osd/ReplicatedPG.h\
	osd/PG.h\
	osd/OSDMap.h\
	os/FileStore.h\
	osd/OSD.h\
	osd/osd_types.h\
	osdc/Blinker.h\
	osdc/Journaler.h\
	osdc/Filer.h\
	osdc/ObjectCacher.h\
	osdc/Objecter.h\
	config.h\
	include/intarith.h\
	include/encoding.h\
	include/err.h
