include_directories(SYSTEM "${CMAKE_SOURCE_DIR}/src/rocksdb/include")
set(crimson_alien_srcs
    alien_store.cc)

list(APPEND crimson_alien_srcs
  ${PROJECT_SOURCE_DIR}/src/common/admin_socket.cc
  ${PROJECT_SOURCE_DIR}/src/common/blkdev.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_context.cc
  ${PROJECT_SOURCE_DIR}/src/common/ceph_crypto.cc
  ${PROJECT_SOURCE_DIR}/src/common/condition_variable_debug.cc
  ${PROJECT_SOURCE_DIR}/src/common/cmdparse.cc
  ${PROJECT_SOURCE_DIR}/src/common/Finisher.cc
  ${PROJECT_SOURCE_DIR}/src/common/HeartbeatMap.cc
  ${PROJECT_SOURCE_DIR}/src/common/PluginRegistry.cc
  ${PROJECT_SOURCE_DIR}/src/common/lockdep.cc
  ${PROJECT_SOURCE_DIR}/src/common/mutex_debug.cc
  ${PROJECT_SOURCE_DIR}/src/common/perf_counters.cc
  ${PROJECT_SOURCE_DIR}/src/common/perf_counters_collection.cc
  ${PROJECT_SOURCE_DIR}/src/common/RefCountedObj.cc
  ${PROJECT_SOURCE_DIR}/src/common/shared_mutex_debug.cc
  ${PROJECT_SOURCE_DIR}/src/common/Throttle.cc
  ${PROJECT_SOURCE_DIR}/src/common/Timer.cc
  ${PROJECT_SOURCE_DIR}/src/common/TrackedOp.cc
  ${PROJECT_SOURCE_DIR}/src/common/WorkQueue.cc
  ${PROJECT_SOURCE_DIR}/src/common/util.cc
  ${PROJECT_SOURCE_DIR}/src/global/global_context.cc
  ${PROJECT_SOURCE_DIR}/src/os/ObjectStore.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/Allocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/AvlAllocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BitmapFreelistManager.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueFS.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/bluefs_types.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueRocksEnv.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueStore.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/bluestore_types.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/fastbmap_allocator_impl.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/FreelistManager.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/HybridAllocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/StupidAllocator.cc
  ${PROJECT_SOURCE_DIR}/src/os/bluestore/BitmapAllocator.cc)

add_library(crimson-alienstore STATIC ${crimson_alien_srcs}
  $<TARGET_OBJECTS:compressor_objs>
  $<TARGET_OBJECTS:crush_objs>
  $<TARGET_OBJECTS:common_prioritycache_obj>)

target_compile_definitions(crimson-alienstore PRIVATE -DWITH_SEASTAR -DWITH_ALIEN)
target_include_directories(crimson-alienstore PRIVATE
  $<TARGET_PROPERTY:Seastar::seastar,INTERFACE_INCLUDE_DIRECTORIES>)
target_link_libraries(crimson-alienstore fmt::fmt)
target_link_libraries(crimson-alienstore kv)
target_link_libraries(crimson-alienstore heap_profiler)
target_link_libraries(crimson-alienstore ${BLKID_LIBRARIES})
target_link_libraries(crimson-alienstore ${UDEV_LIBRARIES})
target_link_libraries(crimson-alienstore crimson)
target_link_libraries(crimson-alienstore blk)
