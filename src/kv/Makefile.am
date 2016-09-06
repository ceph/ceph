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

ROCKSDBCXX_FLAGS = "-fPIC"
if FREEBSD
NPROC = sysctl -n hw.ncpu
ROCKSDBCXX_FLAGS += "-Wmismatched-tags"
else
NPROC = nproc
endif

# explicitly disable jemalloc unless we are using it.
if !WITH_JEMALLOC
export DISABLE_JEMALLOC=1
endif

rocksdb/librocksdb.a:
	cd rocksdb && CC="${CC}" CXX="${CXX}" EXTRA_CXXFLAGS="${ROCKSDBCXX_FLAGS}" PORTABLE=1 ${MAKE} -j$(shell ${NPROC}) static_lib
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

libkv_a_SOURCES += kv/MemDB.cc
noinst_HEADERS += kv/MemDB.h


endif # ENABLE_SERVER
