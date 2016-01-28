# unittest_crush_wrapper
add_executable(unittest_crush_wrapper EXCLUDE_FROM_ALL
  CrushWrapper.cc
  )
add_ceph_unittest(unittest_crush_wrapper ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_crush_wrapper)
target_link_libraries(unittest_crush_wrapper global crush ${BLKID_LIBRARIES})

# unittest_crush
add_executable(unittest_crush EXCLUDE_FROM_ALL
  crush.cc
  )
add_ceph_unittest(unittest_crush ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_crush)
target_link_libraries(unittest_crush global m ${BLKID_LIBRARIES})
