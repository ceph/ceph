#
# note: the old Makefile is at Makefile.old
#

AUTOMAKE_OPTIONS = gnu

# monitor
cmon_SOURCES = cmon.cc msg/SimpleMessenger.cc
cmon_LDADD = libmon.a libcrush.a libcommon.a 

# admin tools
ceph_SOURCES = ceph.cc msg/SimpleMessenger.cc
ceph_LDADD = libcrush.a libcommon.a -ledit
mkmonfs_SOURCES = mkmonfs.cc
mkmonfs_LDADD = libmon.a libcrush.a libcommon.a
monmaptool_SOURCES = monmaptool.cc
monmaptool_LDADD = libcommon.a
crushtool_SOURCES = crushtool.cc
crushtool_LDADD = libcrush.a libcommon.a 
osdmaptool_SOURCES = osdmaptool.cc
osdmaptool_LDADD = libcrush.a libcommon.a
cconf_SOURCES = cconf.cc
cconf_LDADD = libcommon.a

mount_ceph_SOURCES = mount.ceph.c

# mds
cmds_SOURCES = cmds.cc msg/SimpleMessenger.cc
cmds_LDADD = libmds.a libosdc.a libcrush.a libcommon.a

dumpjournal_SOURCES = dumpjournal.cc msg/SimpleMessenger.cc
dumpjournal_LDADD = libosdc.a libcrush.a libcommon.a

# osd 
cosd_SOURCES = cosd.cc msg/SimpleMessenger.cc objclass/class_debug.cc \
	       objclass/class_api.cc
cosd_LDADD = libosd.a libos.a libcrush.a libcommon.a -ldl
dupstore_SOURCES = dupstore.cc
dupstore_LDADD = libos.a libcommon.a
streamtest_SOURCES = streamtest.cc
streamtest_LDADD = libos.a libcommon.a

# synthetic client
csyn_SOURCES = csyn.cc msg/SimpleMessenger.cc
csyn_LDADD = libclient.a libosdc.a libcrush.a libcommon.a

bin_PROGRAMS = \
	cmon cmds cosd csyn \
	ceph cconf \
	mkmonfs monmaptool osdmaptool crushtool \
	streamtest dupstore dumpjournal testmsgr \
	testrados testradospp

sbin_PROGRAMS = \
	mount.ceph

bin_SCRIPTS = crun

testmsgr_SOURCES = testmsgr.cc msg/SimpleMessenger.cc
testmsgr_LDADD = libcommon.a


# fuse targets?
if WITH_FUSE
cfuse_SOURCES = cfuse.cc msg/SimpleMessenger.cc client/fuse.cc client/fuse_ll.cc
cfuse_LDADD = -lfuse libclient.a libosdc.a libcrush.a libcommon.a
bin_PROGRAMS += cfuse

if WITH_DEBUG
fakefuse_SOURCES = fakefuse.cc msg/FakeMessenger.cc client/fuse.cc client/fuse_ll.cc
fakefuse_LDADD = -lfuse libmon.a libmds.a libosd.a libos.a \
	libclient.a libosdc.a libcrush.a 
bin_PROGRAMS += fakefuse
endif

endif

# debug targets?
if WITH_DEBUG
psim_SOURCES = psim.cc
psim_LDADD = libcrush.a libcommon.a

fakesyn_SOURCES = fakesyn.cc msg/FakeMessenger.cc
fakesyn_LDADD = libmon.a libmds.a libosd.a libos.a \
	libclient.a libosdc.a libcrush.a libcommon.a

bin_PROGRAMS += psim fakesyn
endif


##########
BUILT_SOURCES =
lib_LTLIBRARIES = 

# libcrush.so
libcrush_la_SOURCES = \
	crush/builder.c \
	crush/mapper.c \
	crush/crush.c
libcrush_la_CFLAGS = ${AM_CFLAGS}
lib_LTLIBRARIES += libcrush.la

# librados
librados_la_SOURCES = \
	librados.cc \
	msg/SimpleMessenger.cc \
	osdc/Objecter.cc \
	${libcommon_a_SOURCES}
librados_la_CFLAGS = ${AM_CFLAGS}
librados_la_CXXFLAGS = ${AM_CXXFLAGS}
librados_la_LDFLAGS = -version-info 1:0:0 -export-symbols-regex 'rados_.*'
lib_LTLIBRARIES += librados.la

testrados_SOURCES = testrados.c
testrados_LDADD = librados.la libcrush.la
testradospp_SOURCES = testradospp.cc
testradospp_LDADD = librados.la libcrush.la


## object classes

libtestclass.so: testclass.cc
	${CXX} -fPIC -shared -g -o testclass.so testclass.cc
BUILT_SOURCES += libtestclass.so




## libcephclient.so
#libcephclient_so_a_SOURCES = \
#	client/Client.cc \
#	client/SyntheticClient.cc \
#	client/Trace.cc \
#	msg/Message.cc \
#	common/Logger.cc \
#	common/Clock.cc \
#	common/Timer.cc \
#	common/Finisher.cc \
#	mon/MonMap.cc \
#	mon/MonClient.cc \
#	osd/OSDMap.cc \
#	config.cc \
#	osdc/Objecter.cc \
#	osdc/ObjectCacher.cc \
#	osdc/Filer.cc \
#	osdc/Journaler.cc
#libcephclient_so_a_CXXFLAGS = ${AM_CXXFLAGS} -fPIC
#libcephclient_so_a_CFLAGS = ${AM_CFLAGS} -fPIC

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

AM_CXXFLAGS = -Wall -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -rdynamic
AM_CFLAGS = -Wall -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -rdynamic
AM_LDFLAGS =

noinst_LIBRARIES = \
	libcommon.a libcrush.a \
	libmon.a libmds.a libosdc.a libosd.a libclient.a \
	libos.a

noinst_LIBRARIES +=  #libcephclient_so.a

# extra bits
EXTRA_DIST = verify-mds-journal.sh vstart.sh \
	     crun ceph_common.sh init-ceph mkcephfs make_version

install-data-local:
	$(install_sh_SCRIPT) -m 0755 mkcephfs $(DESTDIR)$(sbindir)/mkcephfs
	mkdir -p $(DESTDIR)$(libdir)/ceph
	$(install_sh_SCRIPT) -m 0755 ceph_common.sh $(DESTDIR)$(libdir)/ceph/ceph_common.sh
	mkdir -p $(DESTDIR)$(sysconfdir)/init.d
	$(install_sh_SCRIPT) -m 0755 init-ceph $(DESTDIR)$(sysconfdir)/init.d/ceph
	$(install_sh_SCRIPT) -m 0600 sample.ceph.conf $(DESTDIR)$(sysconfdir)/ceph/sample.ceph.conf

.make_last_ver:
	./make_version --check

ceph_ver.h : $(all_sources) .make_last_ver
	./make_version

ceph_ver.c : ceph_ver.h

# cleaning
clean-local:
	-rm *.so
#	-rm crush/*.cxx
#	-rm CrushWrapper.pm

# libs
libcommon_a_SOURCES = \
	ceph_ver.c \
	$(libcommon_files)

# this list ommits the ceph_ver.c file
libcommon_files = \
	auth/ExportControl.cc \
	common/LogClient.cc \
	msg/Message.cc \
	common/Logger.cc \
	common/Clock.cc \
	common/Timer.cc \
	common/Finisher.cc \
	common/sctp_crc32.c\
	common/assert.cc \
	common/dyn_snprintf.c \
	common/WorkQueue.cc \
	common/ConfUtils.cc \
	mon/MonMap.cc \
	mon/MonClient.cc \
	osd/OSDMap.cc \
	mds/MDSMap.cc \
	common/tls.cc \
	common/common_init.cc \
	common/buffer.cc \
	common/debug.cc \
	config.cc \
	common/lockdep.cc

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
	mon/LogMonitor.cc \
	mon/ClassMonitor.cc \
	mon/Elector.cc \
	mon/MonitorStore.cc

libmds_a_SOURCES = \
	mds/MDS.cc \
	mds/locks.c \
	mds/journal.cc \
	mds/Server.cc \
	mds/MDCache.cc \
	mds/Locker.cc \
	mds/Migrator.cc \
	mds/MDBalancer.cc \
	mds/CDentry.cc \
	mds/CDir.cc \
	mds/CInode.cc \
	mds/LogEvent.cc \
	mds/MDSTable.cc \
	mds/InoTable.cc \
	mds/MDSTableClient.cc \
	mds/MDSTableServer.cc \
	mds/AnchorServer.cc \
	mds/AnchorClient.cc \
	mds/SnapServer.cc \
	mds/snap.cc \
	mds/SessionMap.cc \
	mds/MDLog.cc

libos_a_SOURCES = \
	os/FileJournal.cc \
	os/FileStore.cc \
	os/JournalingObjectStore.cc

libosd_a_SOURCES = \
	osd/PG.cc \
	osd/ReplicatedPG.cc \
	osd/Ager.cc \
	osd/OSD.cc \
        common/ClassHandler.cc
#	osd/RAID4PG.cc

libosdc_a_SOURCES = \
	osdc/Objecter.cc \
	osdc/ObjectCacher.cc \
	osdc/Filer.cc \
	osdc/Journaler.cc

libclient_a_SOURCES = \
	client/Client.cc \
	client/SyntheticClient.cc \
	client/Trace.cc

# headers... and everything else we want to include in a 'make dist' 
# that autotools doesn't magically identify.
noinst_HEADERS = \
	auth/ExportControl.h\
	ceph_ver.h \
        client/Client.h\
        client/SyntheticClient.h\
        client/Trace.h\
        client/fuse.h\
        client/fuse_ll.h\
        client/hadoop/CephFSInterface.h\
	cm.txt\
	common/debug.h\
	common/lockdep.h\
	common/BackTrace.h\
        common/Clock.h\
        common/common_init.h\
        common/Cond.h\
        common/dyn_snprintf.h\
        common/ClassHandler.h\
        common/ConfUtils.h\
        common/DecayCounter.h\
        common/Finisher.h\
        common/LogType.h\
        common/Logger.h\
        common/Mutex.h\
        common/RWLock.h\
        common/Semaphore.h\
	common/Spinlock.h\
        common/Thread.h\
        common/ThreadPool.h\
        common/Timer.h\
        common/tls.h\
	common/WorkQueue.h\
	common/LogClient.h\
        config.h\
        crush/CrushWrapper.h\
        crush/CrushWrapper.i\
        crush/builder.h\
        crush/crush.h\
        crush/grammar.h\
        crush/hash.h\
        crush/mapper.h\
        crush/sample.txt\
        crush/types.h\
	include/ClassLibrary.h\
        include/Context.h\
        include/Distribution.h\
	include/LogEntry.h\
	include/assert.h\
        include/atomic.h\
        include/bitmapper.h\
        include/blobhash.h\
        include/buffer.h\
        include/byteorder.h\
        include/ceph_fs.h\
	include/color.h\
	include/crc32c.h\
        include/cstring.h\
        include/encoding.h\
        include/err.h\
        include/error.h\
        include/filepath.h\
        include/frag.h\
        include/hash.h\
        include/intarith.h\
        include/interval_set.h\
        include/inttypes.h\
	include/librados.h\
        include/lru.h\
	include/msgr.h\
        include/nstring.h\
        include/object.h\
        include/page.h\
        include/rangeset.h\
	include/rados.h\
        include/statlite.h\
        include/triple.h\
        include/tstring.h\
        include/types.h\
        include/uofs.h\
        include/utime.h\
        include/xlist.h\
	kernel/Kconfig\
        kernel/Makefile\
	kernel/addr.c\
	kernel/caps.c\
	kernel/ceph_debug.h\
	kernel/ceph_fs.h\
	kernel/ceph_ver.h\
        kernel/crush/crush.c\
        kernel/crush/crush.h\
        kernel/crush/hash.h\
        kernel/crush/mapper.c\
        kernel/crush/mapper.h\
	kernel/debugfs.c\
	kernel/decode.h\
	kernel/dir.c\
	kernel/export.c\
	kernel/file.c\
	kernel/inode.c\
	kernel/ioctl.c\
	kernel/ioctl.h\
	kernel/kbuild.patch\
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
	kernel/snap.c\
	kernel/super.c\
	kernel/super.h\
	kernel/types.h\
	mds/locks.c\
	mds/locks.h\
        mds/Anchor.h\
        mds/AnchorClient.h\
        mds/AnchorServer.h\
        mds/CDentry.h\
        mds/CDir.h\
        mds/CInode.h\
        mds/Capability.h\
        mds/InoTable.h\
        mds/LocalLock.h\
        mds/Locker.h\
        mds/LogEvent.h\
        mds/LogSegment.h\
        mds/MDBalancer.h\
        mds/MDCache.h\
        mds/MDLog.h\
        mds/MDS.h\
        mds/MDSMap.h\
	mds/MDSTable.h\
	mds/MDSTableServer.h\
	mds/MDSTableClient.h\
        mds/Migrator.h\
        mds/ScatterLock.h\
        mds/Server.h\
        mds/SessionMap.h\
        mds/SimpleLock.h\
        mds/SnapClient.h\
        mds/SnapServer.h\
        mds/events/ECommitted.h\
        mds/events/EExport.h\
        mds/events/EFragment.h\
        mds/events/EImportFinish.h\
        mds/events/EImportStart.h\
        mds/events/EMetaBlob.h\
        mds/events/EOpen.h\
        mds/events/ESession.h\
        mds/events/ESessions.h\
        mds/events/ESlaveUpdate.h\
        mds/events/EString.h\
        mds/events/ESubtreeMap.h\
	mds/events/ETableClient.h\
	mds/events/ETableServer.h\
        mds/events/EUpdate.h\
        mds/mds_table_types.h\
        mds/mdstypes.h\
        mds/snap.h\
        messages/MCacheExpire.h\
	messages/MClass.h\
	messages/MClassAck.h\
        messages/MClientCaps.h\
        messages/MClientCapRelease.h\
        messages/MClientLease.h\
        messages/MClientMount.h\
        messages/MClientMountAck.h\
        messages/MClientReconnect.h\
        messages/MClientReply.h\
        messages/MClientRequest.h\
        messages/MClientRequestForward.h\
        messages/MClientSession.h\
        messages/MClientSnap.h\
        messages/MClientUnmount.h\
        messages/MDentryUnlink.h\
        messages/MDirUpdate.h\
        messages/MDiscover.h\
        messages/MDiscoverReply.h\
        messages/MExportCaps.h\
        messages/MExportCapsAck.h\
        messages/MExportDir.h\
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
        messages/MHeartbeat.h\
        messages/MInodeFileCaps.h\
        messages/MLock.h\
	messages/MLog.h\
	messages/MLogAck.h\
        messages/MMDSBeacon.h\
        messages/MMDSBoot.h\
        messages/MMDSCacheRejoin.h\
        messages/MMDSFragmentNotify.h\
        messages/MMDSGetMap.h\
        messages/MMDSMap.h\
        messages/MMDSResolve.h\
        messages/MMDSResolveAck.h\
        messages/MMDSSlaveRequest.h\
        messages/MMDSTableRequest.h\
        messages/MMonCommand.h\
        messages/MMonCommandAck.h\
        messages/MMonElection.h\
        messages/MMonGetMap.h\
        messages/MMonMap.h\
        messages/MMonObserve.h\
        messages/MMonObserveNotify.h\
        messages/MMonPaxos.h\
        messages/MOSDAlive.h\
        messages/MOSDBoot.h\
        messages/MOSDFailure.h\
        messages/MOSDGetMap.h\
        messages/MOSDIn.h\
        messages/MOSDMap.h\
        messages/MOSDOp.h\
        messages/MOSDOpReply.h\
        messages/MOSDOut.h\
        messages/MOSDPGCreate.h\
        messages/MOSDPGInfo.h\
        messages/MOSDPGLog.h\
        messages/MOSDPGNotify.h\
        messages/MOSDPGQuery.h\
        messages/MOSDPGRemove.h\
        messages/MOSDPing.h\
	messages/MOSDScrub.h\
        messages/MOSDSubOp.h\
        messages/MOSDSubOpReply.h\
        messages/MPGStats.h\
        messages/MPGStatsAck.h\
        messages/MPing.h\
        messages/MRemoveSnaps.h\
        messages/MStatfs.h\
        messages/MStatfsReply.h\
        mon/ClassMonitor.h\
        mon/ClientMap.h\
        mon/ClientMonitor.h\
        mon/Elector.h\
	mon/LogMonitor.h\
        mon/MDSMonitor.h\
        mon/MonClient.h\
        mon/MonMap.h\
        mon/Monitor.h\
        mon/MonitorStore.h\
        mon/OSDMonitor.h\
        mon/PGMap.h\
        mon/PGMonitor.h\
        mon/Paxos.h\
        mon/PaxosService.h\
        mon/mon_types.h\
        msg/Dispatcher.h\
        msg/FakeMessenger.h\
        msg/Message.h\
        msg/Messenger.h\
        msg/SimpleMessenger.h\
        msg/msg_types.h\
        msg/tcp.cc\
        msg/tcp.h\
        os/BDBMap.h\
        os/Fake.h\
        os/FakeStoreBDBCollections.h\
        os/FileJournal.h\
        os/FileStore.h\
        os/Journal.h\
        os/JournalingObjectStore.h\
        os/ObjectStore.h\
        osbdb/OSBDB.h\
        osd/Ager.h\
        osd/OSD.h\
        osd/OSDMap.h\
        osd/ObjectVersioner.h\
        osd/PG.h\
        osd/RAID4PG.h\
        osd/ReplicatedPG.h\
        osd/osd_types.h\
        osdc/Blinker.h\
        osdc/Filer.h\
        osdc/Journaler.h\
        osdc/ObjectCacher.h\
        osdc/Objecter.h\
	sample.ceph.conf

all_sources = $(cmon_SOURCES) $(ceph_SOURCES) $(mkmonfs_SOURCES) $(monmaptool_SOURCES) \
	$(crushtool_SOURCES) $(osdmaptool_SOURCES) $(cconf_SOURCES) $(mount_ceph_SOURCES) $(cmds_SOURCES) \
	$(dumpjournal_SOURCES) $(cosd_SOURCES) $(dupstore_SOURCES) $(streamtest_SOURCES) $(csyn_SOURCES)  \
	$(testmsgr_SOURCES) $(cfuse_SOURCES) $(fakefuse_SOURCES) $(psim_SOURCES) \
	$(libcrush_so_a_SOURCES) $(libcommon_files) $(libcrush_a_SOURCES)  \
	$(libmon_a_SOURCES) $(libmds_a_SOURCES) $(libos_a_SOURCES) $(libosd_a_SOURCES) \
	$(libosdc_a_SOURCES) $(libclient_a_SOURCES)
