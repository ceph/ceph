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

# unittest_mds_quiesce_db
add_executable(unittest_mds_quiesce_db
  TestQuiesceDb.cc
  ../../../src/mds/QuiesceDbManager.cc
  ../../../src/mds/BoostUrlImpl.cc
  $<TARGET_OBJECTS:unit-main>
)
add_ceph_unittest(unittest_mds_quiesce_db)
target_link_libraries(unittest_mds_quiesce_db ceph-common global)

