libos_la_SOURCES = \
	os/chain_xattr.cc \
	os/BtrfsFileStoreBackend.cc \
	os/DBObjectMap.cc \
	os/FileJournal.cc \
	os/FileStore.cc \
	os/FlatIndex.cc \
	os/GenericFileStoreBackend.cc \
	os/HashIndex.cc \
	os/IndexManager.cc \
	os/JournalingObjectStore.cc \
	os/LevelDBStore.cc \
	os/LFNIndex.cc \
	os/MemStore.cc \
	os/ObjectStore.cc \
	os/WBThrottle.cc \
	os/ZFSFileStoreBackend.cc \
	common/TrackedOp.cc
noinst_LTLIBRARIES += libos.la

noinst_HEADERS += \
	os/btrfs_ioctl.h \
	os/chain_xattr.h \
	os/BtrfsFileStoreBackend.h \
	os/CollectionIndex.h \
	os/DBObjectMap.h \
	os/FileJournal.h \
	os/FileStore.h \
	os/FlatIndex.h \
	os/FDCache.h \
	os/GenericFileStoreBackend.h \
	os/HashIndex.h \
	os/IndexManager.h \
	os/Journal.h \
	os/JournalingObjectStore.h \
	os/KeyValueDB.h \
	os/LevelDBStore.h \
	os/LFNIndex.h \
	os/MemStore.h \
	os/ObjectMap.h \
	os/ObjectStore.h \
	os/SequencerPosition.h \
	os/WBThrottle.h \
	os/ZFSFileStoreBackend.h

if WITH_LIBZFS
libos_zfs_a_SOURCES = os/ZFS.cc
libos_zfs_a_CXXFLAGS = ${AM_CXXFLAGS} ${LIBZFS_CFLAGS}
noinst_LIBRARIES += libos_zfs.a
noinst_HEADERS += os/ZFS.h
endif

