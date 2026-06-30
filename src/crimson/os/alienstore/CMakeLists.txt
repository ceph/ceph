include_directories(SYSTEM "${CMAKE_SOURCE_DIR}/src/rocksdb/include")

set(crimson_alien_common_srcs
  ${PROJECT_SOURCE_DIR}/src/common/admin_socket.cc
  ${PROJECT_SOURCE_DIR}/src/common/url_escape.cc
  ${PROJECT_SOURCE_DIR}/src/common/blkdev.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_context.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_crypto.cc
  ${PROJECT_SOURCE_DIR}/src/common/Finisher.cc
  ${PROJECT_SOURCE_DIR}/src/common/HeartbeatMap.cc
  ${PROJECT_SOURCE_DIR}/src/common/PluginRegistry.cc
  ${PROJECT_SOURCE_DIR}/src/common/perf_counters.cc
  ${PROJECT_SOURCE_DIR}/src/common/perf_counters_collection.cc
  ${PROJECT_SOURCE_DIR}/src/common/RefCountedObj.cc
  ${PROJECT_SOURCE_DIR}/src/common/SubProcess.cc
  ${PROJECT_SOURCE_DIR}/src/common/Throttle.cc
  ${PROJECT_SOURCE_DIR}/src/common/Timer.cc
  ${PROJECT_SOURCE_DIR}/src/common/TrackedOp.cc
  ${PROJECT_SOURCE_DIR}/src/common/WorkQueue.cc
  ${PROJECT_SOURCE_DIR}/src/common/util.cc
  ${PROJECT_SOURCE_DIR}/src/crush/CrushLocation.cc
  ${PROJECT_SOURCE_DIR}/src/global/global_context.cc
  ${PROJECT_SOURCE_DIR}/src/log/Log.cc
  $<TARGET_OBJECTS:compressor_objs>
  $<TARGET_OBJECTS:common_prioritycache_obj>)
if(WITH_CEPH_DEBUG_MUTEX)
  list(APPEND crimson_alien_common_srcs
    ${PROJECT_SOURCE_DIR}/src/common/lockdep.cc
    ${PROJECT_SOURCE_DIR}/src/common/mutex_debug.cc
    ${PROJECT_SOURCE_DIR}/src/common/condition_variable_debug.cc
    ${PROJECT_SOURCE_DIR}/src/common/shared_mutex_debug.cc)
endif()

set(alien_store_srcs
  alien_store.cc
  thread_pool.cc
  alien_log.cc
  ${PROJECT_SOURCE_DIR}/src/os/ObjectStore.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/Allocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/AllocatorBase.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/AvlAllocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BtreeAllocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/Btree2Allocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BitmapFreelistManager.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueFS.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/bluefs_types.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueRocksEnv.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueStore.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/simple_bitmap.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/bluestore_types.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/fastbmap_allocator_impl.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/FreelistManager.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/HybridAllocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/StupidAllocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BitmapAllocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/Writer.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/Compression.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueStore_debug.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueAdmin.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/OnodeScan.cc
  ${PROJECT_SOURCE_DIR}/src/os/memstore/MemStore.cc)

# Merge alienstore + alien-common into a single archive.  The common sources
# are compiled without WITH_CRIMSON and duplicate symbols in crimson-common.
# ld.bfd resolves these by archive order; Mold treats them as errors.
# After building, localize the duplicate symbols so they resolve internally
# within this archive (for the alien/bluestore thread) while crimson-osd's
# own references resolve from crimson-common (for seastar threads).
add_library(crimson-alienstore STATIC
  ${alien_store_srcs}
  ${crimson_alien_common_srcs})
if(WITH_LTTNG)
  add_dependencies(crimson-alienstore bluestore-tp)
endif()
if(WITH_BREAKPAD)
  target_link_libraries(crimson-alienstore PRIVATE Breakpad::client)
endif()

target_link_libraries(crimson-alienstore
  PRIVATE
    seastar
    ${FMT_LIB}
    kv
    heap_profiler
    ${BLKID_LIBRARIES}
    ${UDEV_LIBRARIES}
    blk)

# Mold duplicate-symbol handling is done at the crimson-osd link step
# (see src/crimson/osd/CMakeLists.txt) by ordering crimson-alienstore
# before crimson-common with --allow-multiple-definition.
