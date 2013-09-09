libos_la_SOURCES = \
	os/FileJournal.cc \
	os/FileStore.cc \
	os/chain_xattr.cc \
	os/ObjectStore.cc \
	os/JournalingObjectStore.cc \
	os/LFNIndex.cc \
	os/HashIndex.cc \
	os/IndexManager.cc \
	os/FlatIndex.cc \
	os/DBObjectMap.cc \
	os/LevelDBStore.cc \
	os/WBThrottle.cc \
	os/BtrfsFileStoreBackend.cc \
	os/GenericFileStoreBackend.cc \
	os/ZFSFileStoreBackend.cc
noinst_LTLIBRARIES += libos.la

noinst_HEADERS += \
	os/btrfs_ioctl.h \
	os/chain_xattr.h \
	os/CollectionIndex.h \
	os/FileJournal.h \
	os/FileStore.h \
	os/BtrfsFileStoreBackend.h \
	os/GenericFileStoreBackend.h \
	os/ZFSFileStoreBackend.h \
	os/FlatIndex.h \
	os/HashIndex.h \
	os/FDCache.h \
	os/WBThrottle.h \
	os/IndexManager.h \
	os/Journal.h \
	os/JournalingObjectStore.h \
	os/LFNIndex.h \
	os/ObjectStore.h \
	os/SequencerPosition.h \
	os/ObjectMap.h \
	os/DBObjectMap.h \
	os/KeyValueDB.h \
	os/LevelDBStore.h

if WITH_LIBZFS
libos_zfs_a_SOURCES = os/ZFS.cc
libos_zfs_a_CXXFLAGS = ${AM_CXXFLAGS} ${LIBZFS_CFLAGS}
noinst_LIBRARIES += libos_zfs.a
noinst_HEADERS += os/ZFS.h
endif

