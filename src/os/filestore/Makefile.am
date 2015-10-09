if ENABLE_SERVER

noinst_HEADERS += \
	os/filestore/btrfs_ioctl.h \
	os/filestore/chain_xattr.h \
	os/filestore/BtrfsFileStoreBackend.h \
	os/filestore/CollectionIndex.h \
	os/filestore/FileJournal.h \
	os/filestore/FileStore.h \
	os/filestore/FDCache.h \
	os/filestore/GenericFileStoreBackend.h \
	os/filestore/HashIndex.h \
	os/filestore/IndexManager.h \
	os/filestore/Journal.h \
	os/filestore/JournalingObjectStore.h \
	os/filestore/LFNIndex.h \
	os/ObjectStore.h \
	os/filestore/WBThrottle.h \
	os/filestore/XfsFileStoreBackend.h \
	os/filestore/ZFSFileStoreBackend.h \
	os/keyvaluestore/DBObjectMap.h \
	os/keyvaluestore/KeyValueDB.h \
	os/keyvaluestore/LevelDBStore.h

libceph_filestore_la_SOURCES = \
	os/filestore/chain_xattr.cc \
	os/filestore/FileJournal.cc \
	os/filestore/FileStore.cc \
	os/filestore/GenericFileStoreBackend.cc \
	os/filestore/HashIndex.cc \
	os/filestore/IndexManager.cc \
	os/filestore/JournalingObjectStore.cc \
	os/filestore/LFNIndex.cc \
	os/filestore/WBThrottle.cc \
	os/keyvaluestore/KeyValueDB.cc \
	os/keyvaluestore/DBObjectMap.cc \
	os/keyvaluestore/LevelDBStore.cc \
	os/filestore/FileStoreFactory.cc

if LINUX
libceph_filestore_la_SOURCES += os/filestore/BtrfsFileStoreBackend.cc
endif

if WITH_LIBXFS
libceph_filestore_la_SOURCES += os/filestore/XfsFileStoreBackend.cc
endif

if WITH_LIBZFS
libceph_filestore_la_SOURCES += os/filestore/ZFSFileStoreBackend.cc
endif

if WITH_LIBZFS
libceph_filestore_zfs_a_SOURCES = os/filestore/ZFS.cc
libceph_filestore_zfs_a_CXXFLAGS = ${AM_CXXFLAGS} ${LIBZFS_CFLAGS}
noinst_LIBRARIES += libceph_filestore_zfs.a
noinst_HEADERS += os/filestore/ZFS.h
endif

libceph_filestore_rocksdb_la_SOURCES = os/keyvaluestore/RocksDBStore.cc
libceph_filestore_rocksdb_la_CXXFLAGS = ${AM_CXXFLAGS} ${LIBROCKSDB_CFLAGS} -std=gnu++11 -I rocksdb/include
libceph_filestore_rocksdb_la_LIBADD = rocksdb/librocksdb.la
noinst_LTLIBRARIES += libceph_filestore_rocksdb.la
noinst_HEADERS += os/keyvaluestore/RocksDBStore.h

EXTRALIBS += -laio
EXTRALIBS += -ldl -llttng-ust -llttng-ust-tracepoint -lboost_random

libceph_filestore_la_CXXFLAGS = ${AM_CXXFLAGS}
libceph_filestore_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libceph_filestore_la_LIBADD = $(EXTRALIBS)
libceph_filestore_la_LIBADD += $(LIBOS) $(PTHREAD_LIBS)
libceph_filestore_la_LIBADD += libceph_filestore_rocksdb.la
if WITH_LTTNG
libceph_filestore_la_LIBADD += $(LIBOS_TP)
endif

libceph_filestore_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
#libceph_filestore_la_LDFLAGS += -export-symbols-regex '.*__ceph_plugin_.*'
endif

LIBFILESTORE_DEPS += libceph_filestore_rocksdb.la

lib_LTLIBRARIES += libceph_filestore.la
endif # ENABLE_SERVER
