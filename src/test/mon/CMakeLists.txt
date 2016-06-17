# ceph_test_mon_workloadgen
add_executable(ceph_test_mon_workloadgen
  test_mon_workloadgen.cc
  )
target_link_libraries(ceph_test_mon_workloadgen
  os
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_mon_workloadgen
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# ceph_test_mon_msg
add_executable(ceph_test_mon_msg 
  test-mon-msg.cc
  )
target_link_libraries(ceph_test_mon_msg os osdc global ${UNITTEST_LIBS})
set_target_properties(ceph_test_mon_msg PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

#scripts
add_ceph_test(misc.sh ${CMAKE_CURRENT_SOURCE_DIR}/misc.sh)
add_ceph_test(mkfs.sh ${CMAKE_CURRENT_SOURCE_DIR}/mkfs.sh)
add_ceph_test(mon-created-time.sh ${CMAKE_CURRENT_SOURCE_DIR}/mon-created-time.sh)
add_ceph_test(mon-handle-forward.sh ${CMAKE_CURRENT_SOURCE_DIR}/mon-handle-forward.sh)
add_ceph_test(mon-ping.sh ${CMAKE_CURRENT_SOURCE_DIR}/mon-ping.sh)
add_ceph_test(mon-scrub.sh ${CMAKE_CURRENT_SOURCE_DIR}/mon-scrub.sh)
add_ceph_test(osd-crush.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-crush.sh)
add_ceph_test(osd-erasure-code-profile.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-erasure-code-profile.sh)
add_ceph_test(osd-pool-create.sh ${CMAKE_CURRENT_SOURCE_DIR}/osd-pool-create.sh)
add_ceph_test(test_pool_quota.sh ${CMAKE_CURRENT_SOURCE_DIR}/test_pool_quota.sh)

# unittest_mon_moncap
add_executable(unittest_mon_moncap EXCLUDE_FROM_ALL
  moncap.cc
  )
add_ceph_unittest(unittest_mon_moncap ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mon_moncap)
target_link_libraries(unittest_mon_moncap mon global)

# unittest_mon_pgmap
add_executable(unittest_mon_pgmap EXCLUDE_FROM_ALL
  PGMap.cc
  )
add_ceph_unittest(unittest_mon_pgmap ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mon_pgmap)
target_link_libraries(unittest_mon_pgmap mon global)

