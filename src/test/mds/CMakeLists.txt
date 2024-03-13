# unittest_mds_authcap
add_executable(unittest_mds_authcap
  TestMDSAuthCaps.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_mds_authcap)
target_link_libraries(unittest_mds_authcap mds global ${BLKID_LIBRARIES})

# unittest_mds_sessionfilter
add_executable(unittest_mds_sessionfilter
  TestSessionFilter.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_mds_sessionfilter)
target_link_libraries(unittest_mds_sessionfilter mds osdc ceph-common global ${BLKID_LIBRARIES})

# unittest_mds_waitable
add_executable(unittest_mds_waitable
  TestMDSWaitable.cc
  $<TARGET_OBJECTS:unit-main>
)
add_ceph_unittest(unittest_mds_waitable)
target_link_libraries(unittest_mds_waitable ceph-common global)
