# test_rados
add_executable(ceph_test_rados 
  TestRados.cc
  TestOpStat.cc
  Object.cc 
  RadosModel.cc
  )
target_link_libraries(ceph_test_rados
  librados
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS
  ceph_test_rados
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# scripts
add_ceph_test(safe-to-destroy.sh ${CMAKE_CURRENT_SOURCE_DIR}/safe-to-destroy.sh)

# unittest_osdmap
add_executable(unittest_osdmap
  TestOSDMap.cc
  )
add_ceph_unittest(unittest_osdmap)
target_link_libraries(unittest_osdmap global ${BLKID_LIBRARIES})

# unittest_osd_types
add_executable(unittest_osd_types
  types.cc
  )
add_ceph_unittest(unittest_osd_types)
target_link_libraries(unittest_osd_types global)

# unittest_ecbackend
add_executable(unittest_ecbackend
  TestECBackend.cc
  )
add_ceph_unittest(unittest_ecbackend)
target_link_libraries(unittest_ecbackend osd global)

# unittest_osdscrub
add_executable(unittest_osdscrub
  TestOSDScrub.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_osdscrub)
target_link_libraries(unittest_osdscrub osd os global ${CMAKE_DL_LIBS} mon ${BLKID_LIBRARIES})

# unittest_pglog
add_executable(unittest_pglog
  TestPGLog.cc
  $<TARGET_OBJECTS:unit-main>
  $<TARGET_OBJECTS:store_test_fixture>
  )
add_ceph_unittest(unittest_pglog)
target_link_libraries(unittest_pglog osd os global ${CMAKE_DL_LIBS} ${BLKID_LIBRARIES})

# unittest_hitset
add_executable(unittest_hitset
  hitset.cc
  )
add_ceph_unittest(unittest_hitset)
target_link_libraries(unittest_hitset osd global ${BLKID_LIBRARIES})

# unittest_osd_osdcap
add_executable(unittest_osd_osdcap
  osdcap.cc
)
if(HAS_VTA)
  set_source_files_properties(osdcap.cc PROPERTIES
    COMPILE_FLAGS -fno-var-tracking-assignments)
endif()
add_ceph_unittest(unittest_osd_osdcap)
target_link_libraries(unittest_osd_osdcap osd global ${BLKID_LIBRARIES})

# unittest ExtentCache
add_executable(unittest_extent_cache
  test_extent_cache.cc
)
add_ceph_unittest(unittest_extent_cache)
target_link_libraries(unittest_extent_cache osd global ${BLKID_LIBRARIES})

# unittest PGTransaction
add_executable(unittest_pg_transaction
  test_pg_transaction.cc
)
add_ceph_unittest(unittest_pg_transaction)
target_link_libraries(unittest_pg_transaction osd global ${BLKID_LIBRARIES})

# unittest ECTransaction
add_executable(unittest_ec_transaction
  test_ec_transaction.cc
)
add_ceph_unittest(unittest_ec_transaction)
target_link_libraries(unittest_ec_transaction osd global ${BLKID_LIBRARIES})

# unittest_mclock_op_class_queue
add_executable(unittest_mclock_op_class_queue
  TestMClockOpClassQueue.cc
)
add_ceph_unittest(unittest_mclock_op_class_queue)
target_link_libraries(unittest_mclock_op_class_queue
  global osd dmclock os
)

# unittest_mclock_client_queue
add_executable(unittest_mclock_client_queue
  TestMClockClientQueue.cc
)
add_ceph_unittest(unittest_mclock_client_queue)
target_link_libraries(unittest_mclock_client_queue
  global osd dmclock os
)
