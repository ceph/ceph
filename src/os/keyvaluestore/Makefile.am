if ENABLE_SERVER

noinst_HEADERS += \
	os/ObjectStore.h \
	os/keyvaluestore/ObjectMap.h \
	os/keyvaluestore/DBObjectMap.h \
	os/keyvaluestore/GenericObjectMap.h \
	os/keyvaluestore/KeyValueDB.h \
	os/keyvaluestore/LevelDBStore.h \
	os/keyvaluestore/KeyValueStore.h 


libceph_keyvaluestore_la_SOURCES = \
	os/keyvaluestore/DBObjectMap.cc \
	os/keyvaluestore/GenericObjectMap.cc \
	os/keyvaluestore/LevelDBStore.cc \
	os/keyvaluestore/KeyValueDB.cc \
	os/keyvaluestore/KeyValueStore.cc \
	os/keyvaluestore/KeyValueStoreFactory.cc

libceph_keyvaluestore_la_CXXFLAGS = ${AM_CXXFLAGS}
libceph_keyvaluestore_la_LIBADD =  $(PTHREAD_LIBS) $(EXTRALIBS) $(LIBCOMMON) $(LIBOS)
libceph_keyvaluestore_la_LIBADD +=  -lpthread -lrt -lsnappy -lz -lbz2 -llz4
libceph_keyvaluestore_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0

if LINUX
libceph_keyvaluestore_la_LDFLAGS += -export-symbols-regex '.*__ceph_plugin_.*'
endif

if WITH_DLIBROCKSDB
libceph_keyvaluestore_rocksdb_la_SOURCES = os/keyvaluestore/RocksDBStore.cc
libceph_keyvaluestore_rocksdb_la_CXXFLAGS = ${AM_CXXFLAGS} ${LIBROCKSDB_CFLAGS} -std=gnu++11
libceph_keyvaluestore_rocksdb_la_LIBADD = -lrocksdb
noinst_LTLIBRARIES += libceph_keyvaluestore_rocksdb.la
noinst_HEADERS += os/keyvaluestore/RocksDBStore.h
libceph_keyvaluestore_la_LIBADD += libceph_keyvaluestore_rocksdb.la
endif

if WITH_SLIBROCKSDB
# for some stupid reason this needs -fPIC...
# PORTABLE=1 fixes the aarch64 build (-march=native doesn't work there)
rocksdb/librocksdb.a:
	cd rocksdb && EXTRA_CXXFLAGS=-fPIC PORTABLE=1 make -j$(shell nproc) static_lib
libceph_keyvaluestore_rocksdb_la_SOURCES = os/keyvaluestore/RocksDBStore.cc
libceph_keyvaluestore_rocksdb_la_CXXFLAGS = ${AM_CXXFLAGS} ${LIBROCKSDB_CFLAGS} -std=gnu++11 -I rocksdb/include
libceph_keyvaluestore_rocksdb_la_CXXFLAGS +=  -I rocksdb/include -fPIC
libceph_keyvaluestore_rocksdb_la_LIBADD = -lpthread -lrt -lsnappy -lz -lbz2 -llz4
libceph_keyvaluestore_rocksdb_la_LIBADD += -L./rocksdb  -lrocksdb
noinst_LTLIBRARIES += libceph_keyvaluestore_rocksdb.la
noinst_HEADERS += os/keyvaluestore/RocksDBStore.h
libceph_keyvaluestore_la_LIBADD += libceph_keyvaluestore_rocksdb.la
endif

if WITH_KINETIC
libceph_keyvaluestore_la_SOURCES += os/keyvaluestore/KineticStore.cc
libceph_keyvaluestore_la_CXXFLAGS += -std=gnu++11
libceph_keyvaluestore_la_LIBADD += -lkinetic_client -lprotobuf -lglog -lgflags libcrypto.a
noinst_HEADERS += os/keyvaluestore/KineticStore.h
endif
lib_LTLIBRARIES += libceph_keyvaluestore.la

endif # ENABLE_SERVER
