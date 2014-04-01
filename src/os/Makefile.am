libos_types_la_SOURCES = \
	os/Transaction.cc
libos_types_la_CXXFLAGS = ${AM_CXXFLAGS}
noinst_LTLIBRARIES += libos_types.la

if ENABLE_SERVER
libos_la_SOURCES = \
	os/chain_xattr.cc \
	os/DBObjectMap.cc \
	os/GenericObjectMap.cc \
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
	os/KeyValueDB.cc \
	os/KeyValueStore.cc \
	os/ObjectStore.cc \
	os/WBThrottle.cc \
	common/TrackedOp.cc

if LINUX
libos_la_SOURCES += os/BtrfsFileStoreBackend.cc
endif

if WITH_LIBXFS
libos_la_SOURCES += os/XfsFileStoreBackend.cc
endif

if WITH_LIBZFS
libos_la_SOURCES += os/ZFSFileStoreBackend.cc
endif

libos_la_CXXFLAGS = ${AM_CXXFLAGS}
libos_la_LIBADD = $(LIBOS_TYPES)
noinst_LTLIBRARIES += libos.la

if WITH_LIBZFS
libos_zfs_a_SOURCES = os/ZFS.cc
libos_zfs_a_CXXFLAGS = ${AM_CXXFLAGS} ${LIBZFS_CFLAGS}
noinst_LIBRARIES += libos_zfs.a
noinst_HEADERS += os/ZFS.h
endif # WITH_LIBZFS

endif # ENABLE_SERVER

noinst_HEADERS += \
	os/btrfs_ioctl.h \
	os/chain_xattr.h \
	os/BtrfsFileStoreBackend.h \
	os/CollectionIndex.h \
	os/DBObjectMap.h \
	os/GenericObjectMap.h \
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
	os/KeyValueStore.h \
	os/ObjectMap.h \
	os/ObjectStore.h \
	os/SequencerPosition.h \
	os/WBThrottle.h \
	os/XfsFileStoreBackend.h \
	os/ZFSFileStoreBackend.h


