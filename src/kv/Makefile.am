if ENABLE_SERVER

libkv_a_SOURCES = \
	kv/KeyValueDB.cc \
	kv/LevelDBStore.cc
libkv_a_CXXFLAGS = ${AM_CXXFLAGS}
libkv_a_LIBADD =

noinst_LIBRARIES += libkv.a

noinst_HEADERS += \
	kv/KeyValueDB.h \
	kv/LevelDBStore.h

if WITH_SLIBROCKSDB
# build rocksdb with its own makefile
# for some stupid reason this needs -fPIC...
# PORTABLE=1 fixes the aarch64 build (-march=native doesn't work there)
rocksdb/librocksdb.a:
	cd rocksdb && EXTRA_CXXFLAGS=-fPIC PORTABLE=1 make -j$(shell nproc) static_lib
libkv_a_CXXFLAGS += -I rocksdb/include -fPIC
libkv_a_SOURCES += kv/RocksDBStore.cc
libkv_a_LIBADD += rocksdb/librocksdb.a
noinst_HEADERS += kv/RocksDBStore.h
endif

if WITH_DLIBROCKSDB
libkv_a_SOURCES += kv/RocksDBStore.cc
# libkv_a_LIBADD += -lrocksdb
noinst_HEADERS += kv/RocksDBStore.h
endif

if WITH_KINETIC
libkv_a_SOURCES += kv/KineticStore.cc
libkv_a_CXXFLAGS += -std=gnu++11
libkv_a_LIBADD += -lkinetic_client -lprotobuf -lglog -lgflags libcrypto.a
noinst_HEADERS += kv/KineticStore.h
endif

endif # ENABLE_SERVER
