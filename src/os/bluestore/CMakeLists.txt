add_library(bluestore OBJECT
  Allocator.cc
  AllocatorBase.cc
  BitmapFreelistManager.cc
  BlueFS.cc
  bluefs_types.cc
  BlueRocksEnv.cc
  BlueStore.cc
  BlueStore_debug.cc
  simple_bitmap.cc
  bluestore_types.cc
  fastbmap_allocator_impl.cc
  FreelistManager.cc
  StupidAllocator.cc
  BitmapAllocator.cc
  AvlAllocator.cc
  BtreeAllocator.cc
  Btree2Allocator.cc
  HybridAllocator.cc
  Writer.cc
  Compression.cc
  BlueAdmin.cc
  BlueEnv.cc)

target_link_libraries(bluestore
  PRIVATE
    blk heap_profiler kv ${FMT_LIB})

add_dependencies(bluestore compressor_plugins)
if(WITH_LTTNG)
  add_dependencies(bluestore bluestore-tp)
endif()

if(WITH_BLUEFS)
  add_library(bluefs SHARED
    BlueRocksEnv.cc)
  target_include_directories(bluefs SYSTEM PUBLIC
    $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
  target_link_libraries(bluefs global)
  install(TARGETS bluefs DESTINATION lib)
endif()
