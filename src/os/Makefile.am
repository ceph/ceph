libos_types_a_SOURCES = \
	os/Transaction.cc
libos_types_a_CXXFLAGS = ${AM_CXXFLAGS}
noinst_LIBRARIES += libos_types.a

if ENABLE_SERVER

libos_a_SOURCES = \
	os/chain_xattr.cc \
	os/fs/FS.cc \
	os/DBObjectMap.cc \
	os/GenericObjectMap.cc \
	os/FileJournal.cc \
	os/FileStore.cc \
	os/GenericFileStoreBackend.cc \
	os/HashIndex.cc \
	os/IndexManager.cc \
	os/JournalingObjectStore.cc \
	os/LFNIndex.cc \
	os/MemStore.cc \
	os/KeyValueStore.cc \
	os/ObjectStore.cc \
	os/WBThrottle.cc

if LINUX
libos_a_SOURCES += os/BtrfsFileStoreBackend.cc
endif

if WITH_LIBAIO
libos_types_a_SOURCES += os/newstore/newstore_types.cc
libos_a_SOURCES += os/newstore/NewStore.cc
endif

if WITH_LIBXFS
libos_a_SOURCES += \
    os/fs/XFS.cc \
    os/XfsFileStoreBackend.cc
endif

if WITH_LIBZFS
libos_a_SOURCES += os/ZFSFileStoreBackend.cc
endif

libos_a_LIBADD = libos_types.a libkv.a

if WITH_LTTNG
libos_a_LIBADD += $(LIBOS_TP)
endif

noinst_LIBRARIES += libos.a

noinst_HEADERS += \
	os/btrfs_ioctl.h \
	os/chain_xattr.h \
	os/newstore/newstore_types.h \
	os/newstore/NewStore.h \
	os/BtrfsFileStoreBackend.h \
	os/CollectionIndex.h \
	os/DBObjectMap.h \
	os/GenericObjectMap.h \
	os/FileJournal.h \
	os/FileStore.h \
	os/FDCache.h \
	os/fs/FS.h \
	os/fs/XFS.h \
	os/GenericFileStoreBackend.h \
	os/HashIndex.h \
	os/IndexManager.h \
	os/Journal.h \
	os/JournalingObjectStore.h \
	os/LFNIndex.h \
	os/MemStore.h \
	os/KeyValueStore.h \
	os/ObjectMap.h \
	os/ObjectStore.h \
	os/PageSet.h \
	os/SequencerPosition.h \
	os/WBThrottle.h \
	os/XfsFileStoreBackend.h \
	os/ZFSFileStoreBackend.h

if WITH_LIBZFS
libos_zfs_a_SOURCES = os/ZFS.cc
libos_zfs_a_CXXFLAGS = ${AM_CXXFLAGS} ${LIBZFS_CFLAGS}
noinst_LIBRARIES += libos_zfs.a
noinst_HEADERS += os/ZFS.h
endif

endif # ENABLE_SERVER
