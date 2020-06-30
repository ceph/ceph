set(libos_srcs
  ObjectStore.cc
  Transaction.cc
  filestore/chain_xattr.cc
  filestore/BtrfsFileStoreBackend.cc
  filestore/DBObjectMap.cc
  filestore/FileJournal.cc
  filestore/FileStore.cc
  filestore/JournalThrottle.cc
  filestore/GenericFileStoreBackend.cc
  filestore/JournalingObjectStore.cc
  filestore/HashIndex.cc
  filestore/IndexManager.cc
  filestore/LFNIndex.cc
  filestore/WBThrottle.cc
  filestore/os_xattr.c
  memstore/MemStore.cc
  kstore/KStore.cc
  kstore/kstore_types.cc
  fs/FS.cc)

if(WITH_BLUESTORE)
  list(APPEND libos_srcs
    bluestore/Allocator.cc
    bluestore/BitmapFreelistManager.cc
    bluestore/BlueFS.cc
    bluestore/bluefs_types.cc
    bluestore/BlueRocksEnv.cc
    bluestore/BlueStore.cc
    bluestore/bluestore_types.cc
    bluestore/fastbmap_allocator_impl.cc
    bluestore/FreelistManager.cc
    bluestore/StupidAllocator.cc
    bluestore/BitmapAllocator.cc
    bluestore/AvlAllocator.cc
    bluestore/HybridAllocator.cc
  )
endif(WITH_BLUESTORE)

if(HAVE_LIBZBC)
  list(APPEND libos_srcs
    bluestore/ZonedAllocator.cc)
endif()

if(WITH_FUSE)
  list(APPEND libos_srcs
    FuseStore.cc)
endif(WITH_FUSE)

if(HAVE_LIBXFS)
  list(APPEND libos_srcs
    filestore/XfsFileStoreBackend.cc
    fs/XFS.cc)
endif()

if(HAVE_LIBZFS)
  add_library(os_zfs_objs OBJECT
    filestore/ZFSFileStoreBackend.cc
    fs/ZFS.cc)
  target_include_directories(os_zfs_objs SYSTEM PRIVATE
    ${ZFS_INCLUDE_DIRS})
  list(APPEND libos_srcs $<TARGET_OBJECTS:os_zfs_objs>)
endif()

add_library(os STATIC ${libos_srcs})
target_link_libraries(os blk)

target_link_libraries(os heap_profiler kv)

if(WITH_BLUEFS)
  add_library(bluefs SHARED 
    bluestore/BlueRocksEnv.cc)
  target_include_directories(bluefs SYSTEM PUBLIC
    $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
  target_link_libraries(bluefs global)
  install(TARGETS bluefs DESTINATION lib)
endif(WITH_BLUEFS)

if(WITH_FUSE)
  target_link_libraries(os FUSE::FUSE)
endif()

if(HAVE_LIBZFS)
  target_link_libraries(os ${ZFS_LIBRARIES})
endif()

if(WITH_LTTNG)
  add_dependencies(os objectstore-tp)
  add_dependencies(os bluestore-tp)
endif()

target_link_libraries(os kv)

add_dependencies(os compressor_plugins)
add_dependencies(os crypto_plugins)


if(WITH_BLUESTORE)
  add_executable(ceph-bluestore-tool
    bluestore/bluestore_tool.cc)
  target_link_libraries(ceph-bluestore-tool
    os global)
  install(TARGETS ceph-bluestore-tool
    DESTINATION bin)
endif()
