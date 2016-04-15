# unittest_mds_authcap
add_executable(unittest_mds_authcap EXCLUDE_FROM_ALL
  TestMDSAuthCaps.cc
  )
add_ceph_unittest(unittest_mds_authcap ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mds_authcap)
target_link_libraries(unittest_mds_authcap mds global ${BLKID_LIBRARIES})

# unittest_mds_sessionfilter
add_executable(unittest_mds_sessionfilter EXCLUDE_FROM_ALL
  TestSessionFilter.cc
  $<TARGET_OBJECTS:common_util_obj>
  )
add_ceph_unittest(unittest_mds_sessionfilter ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mds_sessionfilter)
target_link_libraries(unittest_mds_sessionfilter mds osdc common global ${BLKID_LIBRARIES})

