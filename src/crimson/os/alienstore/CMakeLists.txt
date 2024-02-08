include_directories(SYSTEM "${CMAKE_SOURCE_DIR}/src/rocksdb/include")

add_library(alien::cflags INTERFACE IMPORTED)
set_target_properties(alien::cflags PROPERTIES
  INTERFACE_COMPILE_DEFINITIONS "WITH_SEASTAR;WITH_ALIEN"
  INTERFACE_INCLUDE_DIRECTORIES $<TARGET_PROPERTY:Seastar::seastar,INTERFACE_INCLUDE_DIRECTORIES>)

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
add_library(crimson-alien-common STATIC
  ${crimson_alien_common_srcs})

target_link_libraries(crimson-alien-common
  crimson-common
  alien::cflags)

set(alien_store_srcs
  alien_store.cc
  thread_pool.cc
  alien_log.cc
  ${PROJECT_SOURCE_DIR}/src/os/ObjectStore.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/Allocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/AvlAllocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BtreeAllocator.cc
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
  ${PROJECT_SOURCE_DIR}/src/os/memstore/MemStore.cc)
add_library(crimson-alienstore STATIC
  ${alien_store_srcs})
if(WITH_LTTNG)
  add_dependencies(crimson-alienstore bluestore-tp)
endif()
target_link_libraries(crimson-alienstore
  PRIVATE
    alien::cflags
    ${FMT_LIB}
    kv
    heap_profiler
    crimson-alien-common
    ${BLKID_LIBRARIES}
    ${UDEV_LIBRARIES}
    crimson
    blk)
