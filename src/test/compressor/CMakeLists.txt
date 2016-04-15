add_library(ceph_example SHARED
  compressor_plugin_example.cc
  ${CMAKE_SOURCE_DIR}/src/compressor/Compressor.cc
  )
target_link_libraries(ceph_example crush pthread ${EXTRA_LIBS})

# unittest_compression_plugin
add_executable(unittest_compression_plugin EXCLUDE_FROM_ALL
  test_compression_plugin.cc
  ${CMAKE_SOURCE_DIR}/src/compressor/Compressor.cc
  )
add_ceph_unittest(unittest_compression_plugin ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_compression_plugin)
target_link_libraries(unittest_compression_plugin 
  osd
  common 
  global 
  dl 
  )

# unittest_compression_snappy
add_executable(unittest_compression_snappy EXCLUDE_FROM_ALL
  test_compression_snappy.cc
  ${snappy_sources}
  )
add_ceph_unittest(unittest_compression_snappy ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_compression_snappy)
target_link_libraries(unittest_compression_snappy 
  osd
  common 
  global 
  dl 
  snappy
  )

# unittest_compression_plugin_snappy
add_executable(unittest_compression_plugin_snappy EXCLUDE_FROM_ALL
  test_compression_plugin_snappy.cc
  ${snappy_sources}
  )
add_ceph_unittest(unittest_compression_plugin_snappy ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_compression_plugin_snappy)
target_link_libraries(unittest_compression_plugin_snappy 
  osd
  common 
  global 
  compressor
  dl 
  snappy
  )

# unittest_compression_zlib
add_executable(unittest_compression_zlib EXCLUDE_FROM_ALL
  test_compression_zlib.cc
  ${CMAKE_SOURCE_DIR}/src/compressor/zlib/CompressionZlib.cc
  )
add_ceph_unittest(unittest_compression_zlib ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_compression_zlib)
target_link_libraries(unittest_compression_zlib 
  z
  osd
  common 
  global 
  )

# unittest_compression_plugin_zlib
add_executable(unittest_compression_plugin_zlib EXCLUDE_FROM_ALL
  test_compression_plugin_zlib.cc
  ${zlib_sources}
  )
add_ceph_unittest(unittest_compression_plugin_zlib ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_compression_plugin_zlib)
target_link_libraries(unittest_compression_plugin_zlib 
  osd
  common 
  global 
  compressor
  dl 
  z
  )

