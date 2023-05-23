if(NOT WIN32)
# get_command_descriptions
# libmon not currently available on Windows.
add_executable(get_command_descriptions
  get_command_descriptions.cc
  $<TARGET_OBJECTS:common_texttable_obj>
  )
target_link_libraries(get_command_descriptions
  mon
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )
endif(NOT WIN32)

# Though FreeBSD has blkdev support, the unittests' mocks only work in Linux
if(HAVE_BLKID AND LINUX)
  # unittest_blkdev
  add_executable(unittest_blkdev
    test_blkdev.cc)
  add_ceph_unittest(unittest_blkdev)
  target_link_libraries(unittest_blkdev global ${BLKID_LIBRARIES})
endif()

# unittest_lockdep
if(WITH_CEPH_DEBUG_MUTEX)
  add_executable(unittest_lockdep
    test_lockdep.cc)
  add_ceph_unittest(unittest_lockdep)
  target_link_libraries(unittest_lockdep ceph-common)
endif()

# unittest_counter
add_executable(unittest_counter
  test_counter.cc)
add_ceph_unittest(unittest_counter)
target_link_libraries(unittest_counter ceph-common)

# FreeBSD only has shims to support NUMA, no functional code.
if(LINUX)
# unittest_numa
add_executable(unittest_numa
  test_numa.cc
  )
add_ceph_unittest(unittest_numa)
target_link_libraries(unittest_numa ceph-common)
endif()

# unittest_bloom_filter
add_executable(unittest_bloom_filter
  test_bloom_filter.cc
  )
add_ceph_unittest(unittest_bloom_filter)
target_link_libraries(unittest_bloom_filter ceph-common)

# unittest_lruset
add_executable(unittest_lruset
  test_lruset.cc
  )
add_ceph_unittest(unittest_lruset)
target_link_libraries(unittest_lruset)

# unittest_histogram
add_executable(unittest_histogram
  histogram.cc
  )
add_ceph_unittest(unittest_histogram)
target_link_libraries(unittest_histogram ceph-common)

# unittest_prioritized_queue
add_executable(unittest_prioritized_queue
  test_prioritized_queue.cc
  )
target_link_libraries(unittest_prioritized_queue ceph-common)
add_ceph_unittest(unittest_prioritized_queue)

# unittest_str_map
add_executable(unittest_str_map
  test_str_map.cc
  )
add_ceph_unittest(unittest_str_map)
target_link_libraries(unittest_str_map ceph-common)

# unittest_json_formattable
add_executable(unittest_json_formattable
  test_json_formattable.cc
  )
add_ceph_unittest(unittest_json_formattable ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_json_formattable)
# add_dependencies(unittest_json_formattable ceph-common)
target_link_libraries(unittest_json_formattable ceph-common global ${BLKID_LIBRARIES})

# unittest_json_formatter
add_executable(unittest_json_formatter
  test_json_formatter.cc
  )
add_ceph_unittest(unittest_json_formatter ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_json_formatter)
# add_dependencies(unittest_json_formatter ceph-common)
target_link_libraries(unittest_json_formatter ceph-common global ${BLKID_LIBRARIES})

# unittest_sharedptr_registry
add_executable(unittest_sharedptr_registry
  test_sharedptr_registry.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_sharedptr_registry)
target_link_libraries(unittest_sharedptr_registry global)

# unittest_shared_cache
add_executable(unittest_shared_cache
  test_shared_cache.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_shared_cache)
target_link_libraries(unittest_shared_cache global)

# unittest_sloppy_crc_map
add_executable(unittest_sloppy_crc_map
  test_sloppy_crc_map.cc
  )
add_ceph_unittest(unittest_sloppy_crc_map)
target_link_libraries(unittest_sloppy_crc_map global)

# unittest_time
add_executable(unittest_time
  test_time.cc
  ${CMAKE_SOURCE_DIR}/src/common/ceph_time.cc
  )
add_ceph_unittest(unittest_time)
target_link_libraries(unittest_time ceph-common)

# unittest_util
add_executable(unittest_util
  test_util.cc
  ${CMAKE_SOURCE_DIR}/src/common/util.cc
  )
add_ceph_unittest(unittest_util)
target_link_libraries(unittest_util global StdFilesystem::filesystem)

# unittest_random
add_executable(unittest_random
  test_random.cc
  )
add_ceph_unittest(unittest_random)
target_link_libraries(unittest_random Boost::random)

# unittest_throttle
add_executable(unittest_throttle
  Throttle.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_throttle PARALLEL)
target_link_libraries(unittest_throttle global) 

# unittest_lru
add_executable(unittest_lru
  test_lru.cc
  )
add_ceph_unittest(unittest_lru)
target_link_libraries(unittest_lru ceph-common)

# unittest_intrusive_lru
add_executable(unittest_intrusive_lru
  test_intrusive_lru.cc
  )
add_ceph_unittest(unittest_intrusive_lru)
target_link_libraries(unittest_intrusive_lru ceph-common)

# unittest_crc32c
add_executable(unittest_crc32c
  test_crc32c.cc
  )
add_ceph_unittest(unittest_crc32c)
target_link_libraries(unittest_crc32c ceph-common)

# unittest_config
add_executable(unittest_config
  test_config.cc
  test_hostname.cc
  )
add_ceph_unittest(unittest_config)
target_link_libraries(unittest_config ceph-common)

# unittest_context
add_executable(unittest_context
  test_context.cc
  )
add_ceph_unittest(unittest_context)
target_link_libraries(unittest_context ceph-common)

# unittest_safe_io
add_executable(unittest_safe_io
  test_safe_io.cc
  )
add_ceph_unittest(unittest_safe_io)
target_link_libraries(unittest_safe_io ceph-common)

# unittest_url_escape
add_executable(unittest_url_escape
  test_url_escape.cc
  )
add_ceph_unittest(unittest_url_escape)
target_link_libraries(unittest_url_escape ceph-common)

# unittest_pretty_binary
add_executable(unittest_pretty_binary
  test_pretty_binary.cc
  )
add_ceph_unittest(unittest_pretty_binary)
target_link_libraries(unittest_pretty_binary ceph-common)

# unittest_readahead
add_executable(unittest_readahead
  Readahead.cc
  )
add_ceph_unittest(unittest_readahead)
target_link_libraries(unittest_readahead ceph-common)

# unittest_tableformatter
add_executable(unittest_tableformatter
  test_tableformatter.cc
  )
add_ceph_unittest(unittest_tableformatter)
target_link_libraries(unittest_tableformatter ceph-common)

add_executable(unittest_xmlformatter
    test_xmlformatter.cc
    )
add_ceph_unittest(unittest_xmlformatter)
target_link_libraries(unittest_xmlformatter ceph-common)

# unittest_bit_vector
add_executable(unittest_bit_vector
  test_bit_vector.cc
  )
add_ceph_unittest(unittest_bit_vector)
target_link_libraries(unittest_bit_vector ceph-common)

# unittest_interval_map
add_executable(unittest_interval_map
  test_interval_map.cc
)
add_ceph_unittest(unittest_interval_map)
target_link_libraries(unittest_interval_map ceph-common)

# unittest_interval_set
add_executable(unittest_interval_set
  test_interval_set.cc
)
add_ceph_unittest(unittest_interval_set)
target_link_libraries(unittest_interval_set ceph-common GTest::Main)

# unittest_weighted_priority_queue
add_executable(unittest_weighted_priority_queue
  test_weighted_priority_queue.cc
  )
target_link_libraries(unittest_weighted_priority_queue ceph-common)
add_ceph_unittest(unittest_weighted_priority_queue)

if(WITH_CEPH_DEBUG_MUTEX)
  add_executable(unittest_mutex_debug
    test_mutex_debug.cc)
  add_ceph_unittest(unittest_mutex_debug)
  target_link_libraries(unittest_mutex_debug ceph-common)
endif()

# unittest_shunique_lock
add_executable(unittest_shunique_lock
  test_shunique_lock.cc
  )
add_ceph_unittest(unittest_shunique_lock)
target_link_libraries(unittest_shunique_lock ceph-common)

add_executable(unittest_fair_mutex
  test_fair_mutex.cc)
add_ceph_unittest(unittest_fair_mutex)
target_link_libraries(unittest_fair_mutex ceph-common)

# unittest_perf_histogram
add_executable(unittest_perf_histogram
  test_perf_histogram.cc
  )
add_ceph_unittest(unittest_perf_histogram)
target_link_libraries(unittest_perf_histogram ceph-common)

# unittest_memory
add_executable(unittest_memory
  test_memory.cc
  )
add_ceph_unittest(unittest_memory)
target_link_libraries(unittest_memory ceph-common)

# unittest_perf_cache_key
add_executable(unittest_perf_counters_key test_perf_counters_key.cc)
add_ceph_unittest(unittest_perf_counters_key)
target_link_libraries(unittest_perf_counters_key ceph-common)

# unittest_global_doublefree
if(WITH_CEPHFS)
  add_executable(unittest_global_doublefree
    test_global_doublefree.cc
    )
  add_ceph_unittest(unittest_global_doublefree)
  target_link_libraries(unittest_global_doublefree cephfs librados ceph-common)
endif(WITH_CEPHFS)

if(NOT WIN32)
add_executable(unittest_dns_resolve
  dns_resolve.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(unittest_dns_resolve global)
add_ceph_unittest(unittest_dns_resolve)
endif()

# We're getting an ICE when trying to compile this test using mingw-gcc and
# recent Boost versions. Note that mingw-llvm works fine.
if (NOT WIN32 OR (NOT(CMAKE_CXX_COMPILER_ID STREQUAL GNU)))
add_executable(unittest_back_trace
  test_back_trace.cc)
set_source_files_properties(test_back_trace.cc PROPERTIES
  COMPILE_FLAGS -fno-inline)
add_ceph_unittest(unittest_back_trace)
target_link_libraries(unittest_back_trace ceph-common)
endif()

add_executable(unittest_hostname
    test_hostname.cc)
add_ceph_unittest(unittest_hostname)
target_link_libraries(unittest_hostname ceph-common)

add_executable(unittest_iso_8601
    test_iso_8601.cc)
add_ceph_unittest(unittest_iso_8601)
target_link_libraries(unittest_iso_8601 ceph-common)

add_executable(unittest_convenience test_convenience.cc)
add_ceph_unittest(unittest_convenience)

add_executable(unittest_bounded_key_counter
  test_bounded_key_counter.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(unittest_bounded_key_counter global)
add_ceph_unittest(unittest_bounded_key_counter)

add_executable(unittest_split test_split.cc)
add_ceph_unittest(unittest_split)

add_executable(unittest_static_ptr test_static_ptr.cc)
add_ceph_unittest(unittest_static_ptr)

add_executable(unittest_hobject test_hobject.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(unittest_hobject global ceph-common)
add_ceph_unittest(unittest_hobject)

add_executable(unittest_async_completion test_async_completion.cc)
add_ceph_unittest(unittest_async_completion)
target_link_libraries(unittest_async_completion ceph-common Boost::system)

add_executable(unittest_async_co_throttle test_async_co_throttle.cc)
add_ceph_unittest(unittest_async_co_throttle)
target_link_libraries(unittest_async_co_throttle ceph-common Boost::system)

add_executable(unittest_async_shared_mutex test_async_shared_mutex.cc)
add_ceph_unittest(unittest_async_shared_mutex)
target_link_libraries(unittest_async_shared_mutex ceph-common Boost::system)

add_executable(unittest_async_parallel_for_each test_async_parallel_for_each.cc)
add_ceph_unittest(unittest_async_parallel_for_each)
target_link_libraries(unittest_async_parallel_for_each ceph-common Boost::system)

add_executable(unittest_async_spawn_group test_async_spawn_group.cc)
add_ceph_unittest(unittest_async_spawn_group)
target_link_libraries(unittest_async_spawn_group ceph-common Boost::system)

add_executable(unittest_cdc test_cdc.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(unittest_cdc global ceph-common)
add_ceph_unittest(unittest_cdc)

add_executable(unittest_ceph_timer test_ceph_timer.cc)
add_ceph_unittest(unittest_ceph_timer)
target_link_libraries(unittest_ceph_timer global ceph-common)

add_executable(unittest_option test_option.cc)
target_link_libraries(unittest_option ceph-common GTest::Main)
add_ceph_unittest(unittest_option)

add_executable(unittest_fault_injector test_fault_injector.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(unittest_fault_injector global)
add_ceph_unittest(unittest_fault_injector)

add_executable(unittest_blocked_completion test_blocked_completion.cc)
add_ceph_unittest(unittest_blocked_completion)
target_link_libraries(unittest_blocked_completion Boost::system GTest::GTest)

add_executable(unittest_allocate_unique test_allocate_unique.cc)
add_ceph_unittest(unittest_allocate_unique)

add_executable(unittest_versioned_variant test_versioned_variant.cc)
add_ceph_unittest(unittest_versioned_variant)
target_link_libraries(unittest_versioned_variant common)

if(WITH_SYSTEMD)
  add_executable(unittest_journald_logger test_journald_logger.cc)
  target_link_libraries(unittest_journald_logger ceph-common)
  add_ceph_unittest(unittest_journald_logger)
endif()

add_executable(unittest_async_call test_async_call.cc)
target_link_libraries(unittest_async_call librados ceph-common Boost::system
  spawn GTest::GTest)
add_ceph_unittest(unittest_async_call)
