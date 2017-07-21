# get_command_descriptions
add_executable(get_command_descriptions
  get_command_descriptions.cc
  $<TARGET_OBJECTS:common_texttable_obj>
  )
target_link_libraries(get_command_descriptions
  mon
  global
  ${LEVELDB_LIBRARIES}
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

if(HAVE_BLKID)
# unittest_blkdev
add_executable(unittest_blkdev
  test_blkdev.cc
  )
add_ceph_unittest(unittest_blkdev ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_blkdev)
target_link_libraries(unittest_blkdev global ${BLKID_LIBRARIES})
endif(HAVE_BLKID)

# unittest_bloom_filter
add_executable(unittest_bloom_filter
  test_bloom_filter.cc
  )
add_ceph_unittest(unittest_bloom_filter ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_bloom_filter)
target_link_libraries(unittest_bloom_filter global ${BLKID_LIBRARIES}) 

# unittest_histogram
add_executable(unittest_histogram
  histogram.cc
  )
add_ceph_unittest(unittest_histogram ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_histogram)
target_link_libraries(unittest_histogram global ${BLKID_LIBRARIES})

# unittest_prioritized_queue
add_executable(unittest_prioritized_queue
  test_prioritized_queue.cc
  )
add_ceph_unittest(unittest_prioritized_queue ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_prioritized_queue)
target_link_libraries(unittest_prioritized_queue global ${BLKID_LIBRARIES})

# unittest_mclock_priority_queue
add_executable(unittest_mclock_priority_queue EXCLUDE_FROM_ALL
  test_mclock_priority_queue.cc
  )
add_ceph_unittest(unittest_mclock_priority_queue ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mclock_priority_queue)
target_link_libraries(unittest_mclock_priority_queue global ${BLKID_LIBRARIES} dmclock)

# unittest_str_map
add_executable(unittest_str_map
  test_str_map.cc
  )
add_ceph_unittest(unittest_str_map ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_str_map)
target_link_libraries(unittest_str_map ceph-common global ${BLKID_LIBRARIES})

# unittest_sharedptr_registry
add_executable(unittest_sharedptr_registry
  test_sharedptr_registry.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_sharedptr_registry ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_sharedptr_registry)
target_link_libraries(unittest_sharedptr_registry global ${BLKID_LIBRARIES})

# unittest_shared_cache
add_executable(unittest_shared_cache
  test_shared_cache.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_shared_cache ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_shared_cache)
target_link_libraries(unittest_shared_cache global ${BLKID_LIBRARIES})

# unittest_sloppy_crc_map
add_executable(unittest_sloppy_crc_map
  test_sloppy_crc_map.cc
  )
add_ceph_unittest(unittest_sloppy_crc_map ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_sloppy_crc_map)
target_link_libraries(unittest_sloppy_crc_map global ${BLKID_LIBRARIES})

# unittest_time
add_executable(unittest_time
  test_time.cc
  ${CMAKE_SOURCE_DIR}/src/common/ceph_time.cc
  )
add_ceph_unittest(unittest_time ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_time)
target_link_libraries(unittest_time global ${BLKID_LIBRARIES})

# unittest_util
add_executable(unittest_util
  test_util.cc
  ${CMAKE_SOURCE_DIR}/src/common/util.cc
  )
add_ceph_unittest(unittest_util ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_util)
target_link_libraries(unittest_util global ${BLKID_LIBRARIES})

# unittest_throttle
add_executable(unittest_throttle
  Throttle.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_throttle ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_throttle)
target_link_libraries(unittest_throttle global) 

# unittest_lru
add_executable(unittest_lru
  test_lru.cc
  )
add_ceph_unittest(unittest_lru ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_lru)
target_link_libraries(unittest_lru global)

# unittest_io_priority
add_executable(unittest_io_priority
  test_io_priority.cc
  )
add_ceph_unittest(unittest_io_priority ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_io_priority)
target_link_libraries(unittest_io_priority global)

# unittest_crc32c
add_executable(unittest_crc32c
  test_crc32c.cc
  )
add_ceph_unittest(unittest_crc32c ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_crc32c)
target_link_libraries(unittest_crc32c global)

# unittest_config
add_executable(unittest_config
  test_config.cc
  test_hostname.cc
  )
add_ceph_unittest(unittest_config ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_config)
target_link_libraries(unittest_config global)

# unittest_context
add_executable(unittest_context
  test_context.cc
  )
add_ceph_unittest(unittest_context ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_context)
target_link_libraries(unittest_context global)

# unittest_safe_io
add_executable(unittest_safe_io
  test_safe_io.cc
  )
add_ceph_unittest(unittest_safe_io ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_safe_io)
target_link_libraries(unittest_safe_io global)

# unittest_url_escape
add_executable(unittest_url_escape
  test_url_escape.cc
  )
add_ceph_unittest(unittest_url_escape ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_url_escape)
target_link_libraries(unittest_url_escape ceph-common)

# unittest_readahead
add_executable(unittest_readahead
  Readahead.cc
  )
add_ceph_unittest(unittest_readahead ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_readahead)
target_link_libraries(unittest_readahead global)

# unittest_tableformatter
add_executable(unittest_tableformatter
  test_tableformatter.cc
  )
add_ceph_unittest(unittest_tableformatter ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_tableformatter)
target_link_libraries(unittest_tableformatter global)

add_executable(unittest_xmlformatter
    test_xmlformatter.cc
    )
add_ceph_unittest(unittest_xmlformatter ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_xmlformatter)
target_link_libraries(unittest_xmlformatter ceph-common)

# unittest_bit_vector
add_executable(unittest_bit_vector
  test_bit_vector.cc
  )
add_ceph_unittest(unittest_bit_vector ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_bit_vector)
target_link_libraries(unittest_bit_vector global)

# unittest_async_compressor
# the test is disabled, because async_compressor is not used anywhere yet.
add_executable(unittest_async_compressor
  test_async_compressor.cc)
target_link_libraries(unittest_async_compressor global ${UNITTEST_LIBS})
add_dependencies(unittest_async_compressor ceph_snappy)
set_target_properties(unittest_async_compressor PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_interval_map
add_executable(unittest_interval_map
  test_interval_map.cc
)
add_ceph_unittest(unittest_interval_map ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_interval_map)
target_link_libraries(unittest_interval_map global)

# unittest_interval_set
add_executable(unittest_interval_set
  test_interval_set.cc
)
add_ceph_unittest(unittest_interval_set ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_interval_set)
target_link_libraries(unittest_interval_set global)

# unittest_weighted_priority_queue
add_executable(unittest_weighted_priority_queue
  test_weighted_priority_queue.cc
  )
add_ceph_unittest(unittest_weighted_priority_queue ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_weighted_priority_queue)
target_link_libraries(unittest_weighted_priority_queue global ${BLKID_LIBRARIES}) 

# unittest_mutex_debug
add_executable(unittest_mutex_debug
  test_mutex_debug.cc
  )
add_ceph_unittest(unittest_mutex_debug ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mutex_debug)
target_link_libraries(unittest_mutex_debug global ${BLKID_LIBRARIES} ${EXTRALIBS})

# unittest_mutex
add_executable(unittest_mutex
  test_mutex.cc
  )
add_ceph_unittest(unittest_mutex ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mutex)
target_link_libraries(unittest_mutex global ${BLKID_LIBRARIES} ${EXTRALIBS})

# unittest_shunique_lock
add_executable(unittest_shunique_lock
  test_shunique_lock.cc
  )
add_ceph_unittest(unittest_shunique_lock ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_shunique_lock)
target_link_libraries(unittest_shunique_lock global ${BLKID_LIBRARIES} ${EXTRALIBS})

# unittest_perf_histogram
add_executable(unittest_perf_histogram
  test_perf_histogram.cc
  )
add_ceph_unittest(unittest_perf_histogram ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_perf_histogram)
target_link_libraries(unittest_perf_histogram global ${BLKID_LIBRARIES} ${EXTRALIBS})

# unittest_global_doublefree
if(WITH_CEPHFS)
  add_executable(unittest_global_doublefree
    test_global_doublefree.cc
    )
  add_ceph_unittest(unittest_global_doublefree ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_global_doublefree)
  target_link_libraries(unittest_global_doublefree cephfs librados)
endif(WITH_CEPHFS)

add_executable(unittest_dns_resolve
  dns_resolve.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(unittest_dns_resolve global)
add_ceph_unittest(unittest_dns_resolve
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_dns_resolve)

add_executable(unittest_back_trace
  test_back_trace.cc)
set_source_files_properties(test_back_trace.cc PROPERTIES
  COMPILE_FLAGS -fno-inline)
target_link_libraries(unittest_back_trace ceph-common)
add_ceph_unittest(unittest_back_trace
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_back_trace)

add_executable(unittest_hostname
    test_hostname.cc)
target_link_libraries(unittest_hostname ceph-common)
add_ceph_unittest(unittest_hostname
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_hostname)

add_executable(unittest_iso_8601
    test_iso_8601.cc)
target_link_libraries(unittest_iso_8601 ceph-common)
add_ceph_unittest(unittest_iso_8601
  ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_hostname)
