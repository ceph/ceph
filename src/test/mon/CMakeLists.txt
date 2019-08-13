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

# unittest_mon_moncap
add_executable(unittest_mon_moncap
  moncap.cc
  )
add_ceph_unittest(unittest_mon_moncap)
target_link_libraries(unittest_mon_moncap mon global)

# unittest_mon_pgmap
add_executable(unittest_mon_pgmap
  PGMap.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_mon_pgmap)
target_link_libraries(unittest_mon_pgmap mon global)

# unittest_mon_montypes
add_executable(unittest_mon_montypes
  test_mon_types.cc
  )
add_ceph_unittest(unittest_mon_montypes)
target_link_libraries(unittest_mon_montypes mon global)

# ceph_test_mon_memory_target
add_executable(ceph_test_mon_memory_target
  test_mon_memory_target.cc
  )
target_link_libraries(ceph_test_mon_memory_target ${UNITTEST_LIBS} Boost::system)
install(TARGETS ceph_test_mon_memory_target
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# ceph_test_mon_log_rss_usage
add_executable(ceph_test_log_rss_usage
  test_log_rss_usage.cc
  )
target_link_libraries(ceph_test_log_rss_usage ${UNITTEST_LIBS})
install(TARGETS ceph_test_log_rss_usage
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# ceph_test_mon_rss_usage
add_executable(ceph_test_mon_rss_usage
  test_mon_rss_usage.cc
  )
target_link_libraries(ceph_test_mon_rss_usage ${UNITTEST_LIBS})
install(TARGETS ceph_test_mon_rss_usage
  DESTINATION ${CMAKE_INSTALL_BINDIR})
