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
  cls_cas_internal
  cls_cas_client
  )
install(TARGETS
  ceph_test_rados
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_rados_io_sequence
  ${CMAKE_CURRENT_SOURCE_DIR}/ceph_test_rados_io_sequence/ceph_test_rados_io_sequence.cc)
add_dependencies(ceph_test_rados_io_sequence erasure_code_plugins)
target_link_libraries(ceph_test_rados_io_sequence
  librados global object_io_exerciser json_structures ec_consistency)
install(TARGETS
  ceph_test_rados_io_sequence
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# test_stale_read
add_executable(ceph_test_osd_stale_read
  ceph_test_osd_stale_read.cc
  )
target_link_libraries(ceph_test_osd_stale_read
  librados
  global
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_osd_stale_read
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

# unittest_ecbackend_l (legacy EC)
add_executable(unittest_ecbackend_l
  TestECBackendL.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_ecbackend_l)
target_link_libraries(unittest_ecbackend_l osd global)

# unittest_ecbackend
add_executable(unittest_ecbackend
  TestECBackend.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_ecbackend)
target_link_libraries(unittest_ecbackend osd global)

# unittest_ecutil
add_executable(unittest_ecutil
        TestECUtil.cc
        $<TARGET_OBJECTS:unit-main>
        $<TARGET_OBJECTS:erasure_code_objs>
)
add_ceph_unittest(unittest_ecutil)
target_link_libraries(unittest_ecutil osd global)

# unittest_osdscrub
add_executable(unittest_osdscrub
  TestOSDScrub.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_osdscrub)
target_link_libraries(unittest_osdscrub osd os global ${CMAKE_DL_LIBS} mon ${BLKID_LIBRARIES})

# unittest_scrubber_be
add_executable(unittest_scrubber_be
  test_scrubber_be.cc
  scrubber_generators.cc
  scrubber_test_datasets.cc
  )
add_ceph_unittest(unittest_scrubber_be)
target_link_libraries(unittest_scrubber_be osd os global ${CMAKE_DL_LIBS} mon ${BLKID_LIBRARIES})

# unittest_pglog
add_executable(unittest_pglog
  TestPGLog.cc
  $<TARGET_OBJECTS:unit-main>
  $<TARGET_OBJECTS:store_test_fixture>
  )
add_ceph_unittest(unittest_pglog)
target_link_libraries(unittest_pglog osd os global ${CMAKE_DL_LIBS} ${BLKID_LIBRARIES})
# unittest_peeringstate
add_executable(unittest_peeringstate
  TestPeeringState.cc
  $<TARGET_OBJECTS:pg_backend_test_fixture>
  $<TARGET_OBJECTS:ec_peering_test_fixture>
  )
add_ceph_unittest(unittest_peeringstate)
target_link_libraries(unittest_peeringstate osd os global ${CMAKE_DL_LIBS} ${BLKID_LIBRARIES})
# pg_backend_test_fixture: object library for PGBackendTestFixture implementation
add_library(pg_backend_test_fixture OBJECT
  PGBackendTestFixture.cc
)
target_link_libraries(pg_backend_test_fixture osd os global GTest::GTest)

# ec_peering_test_fixture: object library for ECPeeringTestFixture implementation
add_library(ec_peering_test_fixture OBJECT
  ECPeeringTestFixture.cc
  MockPeeringListener.cc
)
target_link_libraries(ec_peering_test_fixture osd os global GTest::GTest)

# unittest_backend_basics (replaces unittest_ecbasics + unittest_replicatedbasics)
add_executable(unittest_backend_basics
  TestBackendBasics.cc
  $<TARGET_OBJECTS:unit-main>
  $<TARGET_OBJECTS:pg_backend_test_fixture>
  )
add_ceph_unittest(unittest_backend_basics)
target_link_libraries(unittest_backend_basics osd os global ${CMAKE_DL_LIBS} ${BLKID_LIBRARIES})
add_dependencies(unittest_backend_basics ec_isa ec_jerasure)
# unittest_ecfailover_with_peering
add_executable(unittest_ecfailover_with_peering
  TestECFailoverWithPeering.cc
  $<TARGET_OBJECTS:unit-main>
  $<TARGET_OBJECTS:store_test_fixture>
  $<TARGET_OBJECTS:pg_backend_test_fixture>
  $<TARGET_OBJECTS:ec_peering_test_fixture>
  )
add_ceph_unittest(unittest_ecfailover_with_peering)
target_link_libraries(unittest_ecfailover_with_peering osd os global ${CMAKE_DL_LIBS} ${BLKID_LIBRARIES})
add_dependencies(unittest_ecfailover_with_peering ec_isa ec_jerasure)
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

# unittest ExtentCache (Legacy)
add_executable(unittest_extent_cache_l
  test_extent_cache_l.cc
)
add_ceph_unittest(unittest_extent_cache_l)
target_link_libraries(unittest_extent_cache_l osd global ${BLKID_LIBRARIES})

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

# unittest ECTransaction (Legacy)
add_executable(unittest_ec_transaction_l
  test_ec_transaction_l.cc
)
add_ceph_unittest(unittest_ec_transaction_l)
target_link_libraries(unittest_ec_transaction_l osd global ${BLKID_LIBRARIES})

# unittest ECTransaction
add_executable(unittest_ec_transaction
  test_ec_transaction.cc
)
add_ceph_unittest(unittest_ec_transaction)
target_link_libraries(unittest_ec_transaction osd global ${BLKID_LIBRARIES})

# unittest_mclock_scheduler
# unittest_mclock_scheduler
add_executable(unittest_mclock_scheduler
  TestMClockScheduler.cc
)
add_ceph_unittest(unittest_mclock_scheduler)
target_link_libraries(unittest_mclock_scheduler
  global osd dmclock os
)

# osd_unittests: custom target that builds and runs all OSD unit tests
# Not including unittest_osdmap, as it is slow. It is tested elsewhere.
set(OSD_UNITTESTS
  unittest_backend_basics
  unittest_ec_transaction
  unittest_ec_transaction_l
  unittest_ecbackend
  unittest_ecbackend_l
  unittest_ecfailover_with_peering
  unittest_ecutil
  unittest_extent_cache
  unittest_extent_cache_l
  unittest_hitset
  unittest_mclock_scheduler
  unittest_osd_osdcap
  unittest_osd_types
  unittest_osdscrub
  unittest_peeringstate
  unittest_pg_transaction
  unittest_pglog
  unittest_scrubber_be
)

# osd_unittests: build all OSD unit tests and run them via ctest.
# This is for development convenience only, it is not used as part of make
# check.
# Using ctest ensures:
#   - All tests run even if one fails (--no-tests-on-failure continues)
#   - Output is shown for failing tests (--output-on-failure)
#   - Adding a new test only requires adding it to OSD_UNITTESTS above
#   - Excludes unittest_osdmap because it is relatively slow.
string(JOIN "|" OSD_UNITTEST_REGEX ${OSD_UNITTESTS})
add_custom_target(osd_unittests
  DEPENDS ${OSD_UNITTESTS}
  COMMAND ${CMAKE_CTEST_COMMAND}
    --test-dir ${CMAKE_BINARY_DIR}
    -R "^(${OSD_UNITTEST_REGEX})$"
    --output-on-failure
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  COMMENT "Building and running all OSD unit tests via ctest"
  VERBATIM
)
