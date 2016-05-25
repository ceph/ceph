libos_types_a_SOURCES = \
	os/kstore/kstore_types.cc \
	os/Transaction.cc
libos_types_a_CXXFLAGS = ${AM_CXXFLAGS}
noinst_LIBRARIES += libos_types.a

if WITH_LIBAIO
libos_types_a_SOURCES += \
	os/bluestore/bluestore_types.cc \
	os/bluestore/bluefs_types.cc
endif

if ENABLE_SERVER

libos_a_SOURCES = \
	os/filestore/chain_xattr.cc \
	os/filestore/DBObjectMap.cc \
	os/filestore/FileJournal.cc \
	os/filestore/FileStore.cc \
	os/filestore/JournalThrottle.cc \
	os/filestore/GenericFileStoreBackend.cc \
	os/filestore/HashIndex.cc \
	os/filestore/IndexManager.cc \
	os/filestore/JournalingObjectStore.cc \
	os/filestore/LFNIndex.cc \
	os/filestore/WBThrottle.cc \
	os/fs/FS.cc \
	os/kstore/kv.cc \
	os/kstore/KStore.cc \
	os/memstore/MemStore.cc \
	os/ObjectStore.cc

if WITH_FUSE
libos_a_SOURCES += os/FuseStore.cc
endif

if WITH_LIBAIO
libos_a_SOURCES += \
	os/bluestore/kv.cc \
	os/bluestore/Allocator.cc \
	os/bluestore/BitmapFreelistManager.cc \
	os/bluestore/BlockDevice.cc \
	os/bluestore/BlueFS.cc \
	os/bluestore/BlueRocksEnv.cc \
	os/bluestore/BlueStore.cc \
	os/bluestore/ExtentFreelistManager.cc \
	os/bluestore/FreelistManager.cc \
	os/bluestore/KernelDevice.cc \
	os/bluestore/BitMapAllocator.cc \
	os/bluestore/BitAllocator.cc \
	os/bluestore/StupidAllocator.cc
endif

if LINUX
libos_a_SOURCES += os/filestore/BtrfsFileStoreBackend.cc
endif

if WITH_LIBXFS
libos_a_SOURCES += \
    os/filestore/XfsFileStoreBackend.cc \
    os/fs/XFS.cc
endif

if WITH_LIBZFS
libos_a_SOURCES += os/filestore/ZFSFileStoreBackend.cc
endif

libos_a_CXXFLAGS = ${AM_CXXFLAGS} -I rocksdb/include -fPIC
libos_a_LIBADD = libos_types.a libkv.a
noinst_LIBRARIES += libos.a

if WITH_LTTNG
libos_a_LIBADD += $(LIBOS_TP)
endif

noinst_HEADERS += \
	os/filestore/chain_xattr.h \
	os/filestore/BtrfsFileStoreBackend.h \
	os/filestore/CollectionIndex.h \
	os/filestore/DBObjectMap.h \
	os/filestore/FileJournal.h \
	os/filestore/FileStore.h \
	os/filestore/JournalThrottle.h \
	os/filestore/FDCache.h \
	os/filestore/GenericFileStoreBackend.h \
	os/filestore/HashIndex.h \
	os/filestore/IndexManager.h \
	os/filestore/Journal.h \
	os/filestore/JournalingObjectStore.h \
	os/filestore/LFNIndex.h \
	os/filestore/SequencerPosition.h \
	os/filestore/WBThrottle.h \
	os/filestore/XfsFileStoreBackend.h \
	os/filestore/ZFSFileStoreBackend.h \
	os/fs/btrfs_ioctl.h \
	os/fs/FS.h \
	os/fs/XFS.h \
	os/kstore/kstore_types.h \
	os/kstore/KStore.h \
	os/kstore/kv.h \
	os/memstore/MemStore.h \
	os/memstore/PageSet.h \
	os/FuseStore.h \
	os/ObjectMap.h \
	os/ObjectStore.h

if WITH_LIBAIO
noinst_HEADERS += \
	os/bluestore/bluefs_types.h \
	os/bluestore/bluestore_types.h \
	os/bluestore/kv.h \
	os/bluestore/Allocator.h \
	os/bluestore/BitmapFreelistManager.h \
	os/bluestore/BlockDevice.h \
	os/bluestore/BlueFS.h \
	os/bluestore/BlueRocksEnv.h \
	os/bluestore/BlueStore.h \
	os/bluestore/KernelDevice.h \
	os/bluestore/ExtentFreelistManager.h \
	os/bluestore/FreelistManager.h \
	os/bluestore/BitMapAllocator.h \
	os/bluestore/BitAllocator.h \
	os/bluestore/StupidAllocator.h
endif

if WITH_LIBZFS
libos_zfs_a_SOURCES = os/fs/ZFS.cc
libos_zfs_a_CXXFLAGS = ${AM_CXXFLAGS} ${LIBZFS_CFLAGS}
noinst_LIBRARIES += libos_zfs.a
noinst_HEADERS += os/fs/ZFS.h
endif

if WITH_SPDK

SPDK_SRCDIR = ${top_srcdir}/src/spdk/lib
${SPDK_SRCDIR}/nvme/libspdk_nvme.a:
	$(MAKE) -C ${SPDK_SRCDIR}/nvme DPDK_INC=${LIBDPDK_CFLAGS}
${SPDK_SRCDIR}/memory/libspdk_memory.a:
	$(MAKE) -C ${SPDK_SRCDIR}/memory DPDK_INC=${LIBDPDK_CFLAGS}
${SPDK_SRCDIR}/util/libspdk_util.a:
	$(MAKE) -C ${SPDK_SRCDIR}/util DPDK_INC=${LIBDPDK_CFLAGS}
${SPDK_SRCDIR}/ioat/libspdk_ioat.a:
	$(MAKE) -C ${SPDK_SRCDIR}/ioat DPDK_INC=${LIBDPDK_CFLAGS}
LIBSPDK_CFLAGS = "-I${top_srcdir}/src/spdk/include"
LIBSPDK_LIBS = \
	${top_srcdir}/src/spdk/lib/nvme/libspdk_nvme.a \
	${top_srcdir}/src/spdk/lib/memory/libspdk_memory.a \
	${top_srcdir}/src/spdk/lib/util/libspdk_util.a \
	${top_srcdir}/src/spdk/lib/ioat/libspdk_ioat.a

libos_a_CXXFLAGS += \
	${LIBSPDK_CFLAGS} \
	${LIBDPDK_CFLAGS} \
	${LIBPCIACCESS_CFLAGS}
libos_a_SOURCES += os/bluestore/NVMEDevice.cc
noinst_HEADERS += os/bluestore/NVMEDevice.h
endif

if WITH_LIBAIO
ceph_bluefs_tool_SOURCES = os/bluestore/bluefs_tool.cc
ceph_bluefs_tool_LDADD = $(LIBOS) $(CEPH_GLOBAL)
bin_PROGRAMS += ceph-bluefs-tool
endif

endif # ENABLE_SERVER
