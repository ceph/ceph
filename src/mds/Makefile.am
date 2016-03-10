LIBMDS_SOURCES = \
	mds/Capability.cc \
	mds/MDSDaemon.cc \
	mds/MDSRank.cc \
	mds/Beacon.cc \
	mds/locks.c \
	mds/journal.cc \
	mds/Server.cc \
	mds/Mutation.cc \
	mds/MDCache.cc \
	mds/RecoveryQueue.cc \
	mds/StrayManager.cc \
	mds/Locker.cc \
	mds/Migrator.cc \
	mds/MDBalancer.cc \
	mds/CDentry.cc \
	mds/CDir.cc \
	mds/CInode.cc \
	mds/DamageTable.cc \
	mds/LogEvent.cc \
	mds/MDSTable.cc \
	mds/InoTable.cc \
	mds/JournalPointer.cc \
	mds/MDSTableClient.cc \
	mds/MDSTableServer.cc \
	mds/SimpleLock.cc \
	mds/ScrubStack.cc \
	mds/SnapRealm.cc \
	mds/SnapServer.cc \
	mds/snap.cc \
	mds/SessionMap.cc \
	mds/MDSContext.cc \
	mds/MDSAuthCaps.cc \
	mds/MDLog.cc
LIBMDS_DEPS = $(LIBOSDC)

if ENABLE_CLIENT
include mds/Makefile-client.am
endif

if ENABLE_SERVER
include mds/Makefile-server.am
endif
