if ENABLE_SERVER

libceph_newstore_la_SOURCES = \
	os/fs/FS.cc \
	os/fs/XFS.cc \
	os/newstore/newstore_types.cc \
	os/newstore/NewStore.cc \
	os/newstore/NewStoreFactory.cc \
	os/keyvaluestore/KeyValueDB.cc \
	os/keyvaluestore/LevelDBStore.cc

libceph_newstore_rocksdb_la_SOURCES = os/keyvaluestore/RocksDBStore.cc
libceph_newstore_rocksdb_la_CXXFLAGS = ${AM_CXXFLAGS} ${LIBROCKSDB_CFLAGS} -std=gnu++11 -I rocksdb/include
libceph_newstore_rocksdb_la_LIBADD = rocksdb/librocksdb.la
noinst_LTLIBRARIES += libceph_newstore_rocksdb.la
noinst_HEADERS += os/keyvaluestore/RocksDBStore.h

libceph_newstore_la_CXXFLAGS = ${AM_CXXFLAGS}
libceph_newstore_la_LIBADD = $(LIBCOMMON) $(LIBOS) $(PTHREAD_LIBS) $(EXTRALIBS)
libceph_newstore_la_LIBADD += libceph_newstore_rocksdb.la

libceph_newstore_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
libceph_newstore_la_LDFLAGS += -export-symbols-regex '.*__ceph_plugin_.*'
endif


noinst_HEADERS += \
	os/ObjectStore.h \
	os/fs/FS.h \
	os/fs/XFS.h \
	os/newstore/newstore_types.h \
	os/newstore/NewStore.h \
	os/keyvaluestore/KeyValueDB.h \
	os/keyvaluestore/LevelDBStore.h

lib_LTLIBRARIES += libceph_newstore.la
endif # ENABLE_SERVER
