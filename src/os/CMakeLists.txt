set(libos_srcs
  ObjectStore.cc
  Transaction.cc
  DBObjectMap.cc
  memstore/MemStore.cc
  kstore/KStore.cc
  kstore/kstore_types.cc
  fs/FS.cc)

if(WITH_BLUESTORE)
  list(APPEND libos_srcs
    bluestore/Allocator.cc
    bluestore/AllocatorBase.cc
    bluestore/BitmapFreelistManager.cc
    bluestore/BlueFS.cc
    bluestore/bluefs_types.cc
    bluestore/BlueRocksEnv.cc
    bluestore/BlueStore.cc
    bluestore/BlueStore_debug.cc
    bluestore/simple_bitmap.cc
    bluestore/bluestore_types.cc
    bluestore/fastbmap_allocator_impl.cc
    bluestore/FreelistManager.cc
    bluestore/StupidAllocator.cc
    bluestore/BitmapAllocator.cc
    bluestore/AvlAllocator.cc
    bluestore/BtreeAllocator.cc
    bluestore/Btree2Allocator.cc
    bluestore/HybridAllocator.cc
    bluestore/Writer.cc
  )
endif(WITH_BLUESTORE)

if(WITH_FUSE)
  list(APPEND libos_srcs
    FuseStore.cc)
endif(WITH_FUSE)

if(HAVE_LIBXFS)
  list(APPEND libos_srcs
    fs/XFS.cc)
endif()

add_library(os STATIC ${libos_srcs})
target_link_libraries(os
  legacy-option-headers
  blk
  ${FMT_LIB})

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

if(WITH_LTTNG)
  add_dependencies(os objectstore-tp)
  add_dependencies(os bluestore-tp)
endif()

if(WITH_JAEGER)
  add_dependencies(os jaeger_base)
  target_link_libraries(os jaeger_base)
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
