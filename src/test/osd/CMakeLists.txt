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
add_ceph_test(osd-bench.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-bench.sh) 
add_ceph_test(osd-config.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-config.sh)
add_ceph_test(osd-markdown.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-markdown.sh) 
add_ceph_test(osd-reactivate.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-reactivate.sh) 
add_ceph_test(osd-reuse-id.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-reuse-id.sh)
add_ceph_test(osd-scrub-repair.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-scrub-repair.sh) 
add_ceph_test(osd-scrub-snaps.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-scrub-snaps.sh)
add_ceph_test(osd-copy-from.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-copy-from.sh)
add_ceph_test(osd-fast-mark-down.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-fast-mark-down.sh)

# unittest_osdmap
add_executable(unittest_osdmap
  TestOSDMap.cc
  )
add_ceph_unittest(unittest_osdmap ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_osdmap)
target_link_libraries(unittest_osdmap global ${BLKID_LIBRARIES})

# unittest_osd_types
add_executable(unittest_osd_types
  types.cc
  )
add_ceph_unittest(unittest_osd_types ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_osd_types)
target_link_libraries(unittest_osd_types global)

# unittest_ecbackend
add_executable(unittest_ecbackend
  TestECBackend.cc
  )
add_ceph_unittest(unittest_ecbackend ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_ecbackend)
target_link_libraries(unittest_ecbackend osd global)

# unittest_osdscrub
add_executable(unittest_osdscrub
  TestOSDScrub.cc
  )
add_ceph_unittest(unittest_osdscrub ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_osdscrub)
target_link_libraries(unittest_osdscrub osd global ${CMAKE_DL_LIBS} os mon ${BLKID_LIBRARIES} ${URCU_LIBRARIES})

# unittest_pglog
add_executable(unittest_pglog
  TestPGLog.cc
  )
add_ceph_unittest(unittest_pglog ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_pglog)
target_link_libraries(unittest_pglog osd global ${CMAKE_DL_LIBS} ${BLKID_LIBRARIES})

# unittest_hitset
add_executable(unittest_hitset
  hitset.cc
  )
add_ceph_unittest(unittest_hitset ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_hitset)
target_link_libraries(unittest_hitset osd global ${BLKID_LIBRARIES})

# unittest_osd_osdcap
add_executable(unittest_osd_osdcap
  osdcap.cc
)
add_ceph_unittest(unittest_osd_osdcap ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_osd_osdcap)
target_link_libraries(unittest_osd_osdcap osd global ${BLKID_LIBRARIES})

