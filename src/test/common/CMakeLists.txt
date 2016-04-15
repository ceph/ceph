# get_command_descriptions
add_executable(get_command_descriptions
  get_command_descriptions.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  )
target_link_libraries(get_command_descriptions
  mon
  global
  leveldb
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

# unittest_blkdev
add_executable(unittest_blkdev EXCLUDE_FROM_ALL
  test_blkdev.cc
  )
add_ceph_unittest(unittest_blkdev ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_blkdev)
target_link_libraries(unittest_blkdev global ${BLKID_LIBRARIES})

# unittest_bloom_filter
add_executable(unittest_bloom_filter EXCLUDE_FROM_ALL
  test_bloom_filter.cc
  )
add_ceph_unittest(unittest_bloom_filter ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_bloom_filter)
target_link_libraries(unittest_bloom_filter global ${BLKID_LIBRARIES}) 

# unittest_histogram
add_executable(unittest_histogram EXCLUDE_FROM_ALL
  histogram.cc
  )
add_ceph_unittest(unittest_histogram ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_histogram)
target_link_libraries(unittest_histogram global ${BLKID_LIBRARIES})

# unittest_prioritized_queue
add_executable(unittest_prioritized_queue EXCLUDE_FROM_ALL
  test_prioritized_queue.cc
  )
add_ceph_unittest(unittest_prioritized_queue ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_prioritized_queue)
target_link_libraries(unittest_prioritized_queue global ${BLKID_LIBRARIES})

# unittest_str_map
add_executable(unittest_str_map EXCLUDE_FROM_ALL
  test_str_map.cc
  )
add_ceph_unittest(unittest_str_map ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_str_map)
target_link_libraries(unittest_str_map common global ${BLKID_LIBRARIES})

# unittest_sharedptr_registry
add_executable(unittest_sharedptr_registry EXCLUDE_FROM_ALL
  test_sharedptr_registry.cc
  )
add_ceph_unittest(unittest_sharedptr_registry ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_sharedptr_registry)
target_link_libraries(unittest_sharedptr_registry global ${BLKID_LIBRARIES})

# unittest_shared_cache
add_executable(unittest_shared_cache EXCLUDE_FROM_ALL
  test_shared_cache.cc
  )
add_ceph_unittest(unittest_shared_cache ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_shared_cache)
target_link_libraries(unittest_shared_cache global ${BLKID_LIBRARIES})

# unittest_sloppy_crc_map
add_executable(unittest_sloppy_crc_map EXCLUDE_FROM_ALL
  test_sloppy_crc_map.cc
  )
add_ceph_unittest(unittest_sloppy_crc_map ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_sloppy_crc_map)
target_link_libraries(unittest_sloppy_crc_map global ${BLKID_LIBRARIES})

# unittest_time
add_executable(unittest_time EXCLUDE_FROM_ALL
  test_time.cc
  ${CMAKE_SOURCE_DIR}/src/common/ceph_time.cc
  )
add_ceph_unittest(unittest_time ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_time)
target_link_libraries(unittest_time global ${BLKID_LIBRARIES})

# unittest_util
add_executable(unittest_util EXCLUDE_FROM_ALL
  test_util.cc
  ${CMAKE_SOURCE_DIR}/src/common/util.cc
  )
add_ceph_unittest(unittest_util ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_util)
target_link_libraries(unittest_util global ${BLKID_LIBRARIES})

# unittest_throttle
add_executable(unittest_throttle EXCLUDE_FROM_ALL
  Throttle.cc
  )
add_ceph_unittest(unittest_throttle ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_throttle)
target_link_libraries(unittest_throttle global) 

# unittest_lru
add_executable(unittest_lru EXCLUDE_FROM_ALL
  test_lru.cc
  )
add_ceph_unittest(unittest_lru ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_lru)
target_link_libraries(unittest_lru global)

# unittest_io_priority
add_executable(unittest_io_priority EXCLUDE_FROM_ALL
  test_io_priority.cc
  )
add_ceph_unittest(unittest_io_priority ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_io_priority)
target_link_libraries(unittest_io_priority global)

# unittest_crc32c
add_executable(unittest_crc32c EXCLUDE_FROM_ALL
  test_crc32c.cc
  )
add_ceph_unittest(unittest_crc32c ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_crc32c)
target_link_libraries(unittest_crc32c global)

# unittest_config
add_executable(unittest_config EXCLUDE_FROM_ALL
  test_config.cc
  )
add_ceph_unittest(unittest_config ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_config)
target_link_libraries(unittest_config global)

# unittest_context
add_executable(unittest_context EXCLUDE_FROM_ALL
  test_context.cc
  )
add_ceph_unittest(unittest_context ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_context)
target_link_libraries(unittest_context global)

# unittest_safe_io
add_executable(unittest_safe_io EXCLUDE_FROM_ALL
  test_safe_io.cc
  )
add_ceph_unittest(unittest_safe_io ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_safe_io)
target_link_libraries(unittest_safe_io global)

# unittest_readahead
add_executable(unittest_readahead EXCLUDE_FROM_ALL
  Readahead.cc
  )
add_ceph_unittest(unittest_readahead ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_readahead)
target_link_libraries(unittest_readahead global)

# unittest_tableformatter
add_executable(unittest_tableformatter EXCLUDE_FROM_ALL
  test_tableformatter.cc
  )
add_ceph_unittest(unittest_tableformatter ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_tableformatter)
target_link_libraries(unittest_tableformatter global)

# unittest_bit_vector
add_executable(unittest_bit_vector EXCLUDE_FROM_ALL
  test_bit_vector.cc
  )
add_ceph_unittest(unittest_bit_vector ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_bit_vector)
target_link_libraries(unittest_bit_vector global)

# unittest_async_compressor
add_executable(unittest_async_compressor EXCLUDE_FROM_ALL
  test_async_compressor.cc
)
add_ceph_unittest(unittest_async_compressor ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_async_compressor)
target_link_libraries(unittest_async_compressor global compressor)

# unittest_interval_set
add_executable(unittest_interval_set EXCLUDE_FROM_ALL
  test_interval_set.cc
)
add_ceph_unittest(unittest_interval_set ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_interval_set)
target_link_libraries(unittest_interval_set global)

# unittest_weighted_priority_queue
add_executable(unittest_weighted_priority_queue EXCLUDE_FROM_ALL
  test_weighted_priority_queue.cc
  )
add_ceph_unittest(unittest_weighted_priority_queue ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_weighted_priority_queue)
target_link_libraries(unittest_weighted_priority_queue global ${BLKID_LIBRARIES}) 

# unittest_mutex_debug
add_executable(unittest_mutex_debug EXCLUDE_FROM_ALL
  test_mutex_debug.cc
  )
add_ceph_unittest(unittest_mutex_debug ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mutex_debug)
target_link_libraries(unittest_mutex_debug global ${BLKID_LIBRARIES} ${EXTRALIBS})

# unittest_shunique_lock
add_executable(unittest_shunique_lock EXCLUDE_FROM_ALL
  test_shunique_lock.cc
  )
add_ceph_unittest(unittest_shunique_lock ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_shunique_lock)
target_link_libraries(unittest_shunique_lock global ${BLKID_LIBRARIES} ${EXTRALIBS})
