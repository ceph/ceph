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

# unittest_config_map
add_executable(unittest_config_map
  test_config_map.cc
  )
add_ceph_unittest(unittest_config_map)
target_link_libraries(unittest_config_map mon global)

# unittest_mon_moncap
add_executable(unittest_mon_moncap
  moncap.cc
  )
add_ceph_unittest(unittest_mon_moncap)
target_link_libraries(unittest_mon_moncap mon global)

# unittest_mon_map
add_executable(unittest_mon_monmap
  MonMap.cc
  )
add_ceph_unittest(unittest_mon_monmap)
target_link_libraries(unittest_mon_monmap mon global)

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
  test_mon_memory_target.cc)
target_link_libraries(ceph_test_mon_memory_target Boost::system Threads::Threads)
set_target_properties(ceph_test_mon_memory_target PROPERTIES
  SKIP_RPATH TRUE
  INSTALL_RPATH "")
install(TARGETS ceph_test_mon_memory_target
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# ceph_test_mon_log_rss_usage
add_executable(ceph_test_log_rss_usage
  test_log_rss_usage.cc)
set_target_properties(ceph_test_log_rss_usage PROPERTIES
  SKIP_RPATH TRUE
  INSTALL_RPATH "")
install(TARGETS ceph_test_log_rss_usage
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# ceph_test_mon_rss_usage
add_executable(ceph_test_mon_rss_usage
  test_mon_rss_usage.cc)
set_target_properties(ceph_test_mon_rss_usage PROPERTIES
  SKIP_RPATH TRUE
  INSTALL_RPATH "")
install(TARGETS ceph_test_mon_rss_usage
  DESTINATION ${CMAKE_INSTALL_BINDIR})

#unittest_mon_election
add_executable(unittest_mon_election
  test_election.cc
  )
add_ceph_unittest(unittest_mon_election)
target_link_libraries(unittest_mon_election mon global)
