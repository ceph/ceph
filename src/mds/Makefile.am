LIBMDS_SOURCES = \
	mds/Capability.cc \
	mds/MDS.cc \
	mds/Beacon.cc \
	mds/locks.c \
	mds/journal.cc \
	mds/Server.cc \
	mds/Mutation.cc \
	mds/MDCache.cc \
	mds/RecoveryQueue.cc \
	mds/Locker.cc \
	mds/Migrator.cc \
	mds/MDBalancer.cc \
	mds/CDentry.cc \
	mds/CDir.cc \
	mds/CInode.cc \
	mds/LogEvent.cc \
	mds/MDSTable.cc \
	mds/InoTable.cc \
	mds/JournalPointer.cc \
	mds/MDSTableClient.cc \
	mds/MDSTableServer.cc \
	mds/SnapRealm.cc \
	mds/SnapServer.cc \
	mds/snap.cc \
	mds/SessionMap.cc \
	mds/MDSContext.cc \
	mds/MDSAuthCaps.cc \
	mds/MDLog.cc \
	common/TrackedOp.cc
LIBMDS_DEPS = $(LIBOSDC)

if ENABLE_CLIENT
include mds/Makefile-client.am
endif

if ENABLE_SERVER
include mds/Makefile-server.am
endif
