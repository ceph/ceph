# unittest_mds_types
add_executable(unittest_mds_types EXCLUDE_FROM_ALL
  mds_types.cc
  )
add_ceph_unittest(unittest_mds_types ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mds_types)
target_link_libraries(unittest_mds_types global)
