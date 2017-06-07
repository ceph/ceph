# unittest_lfnindex
add_executable(unittest_lfnindex
  TestLFNIndex.cc
  )
add_ceph_unittest(unittest_lfnindex ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_lfnindex)
target_link_libraries(unittest_lfnindex os global)

