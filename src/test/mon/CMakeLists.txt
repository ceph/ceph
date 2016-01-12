# ceph_test_mon_workloadgen
add_executable(ceph_test_mon_workloadgen
  test_mon_workloadgen.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(ceph_test_mon_workloadgen
  os
  osdc
  global
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_mon_msg
add_executable(ceph_test_mon_msg 
  test-mon-msg.cc
  )
target_link_libraries(ceph_test_mon_msg os osdc global ${UNITTEST_LIBS})
set_target_properties(ceph_test_mon_msg PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

#scripts
add_ceph_test(misc.sh ${CMAKE_SOURCE_DIR}/src/test/mon/misc.sh)
add_ceph_test(mkfs.sh ${CMAKE_SOURCE_DIR}/src/test/mon/mkfs.sh)
add_ceph_test(mon-created-time.sh ${CMAKE_SOURCE_DIR}/src/test/mon/mon-created-time.sh)
add_ceph_test(mon-handle-forward.sh ${CMAKE_SOURCE_DIR}/src/test/mon/mon-handle-forward.sh)
add_ceph_test(mon-ping.sh ${CMAKE_SOURCE_DIR}/src/test/mon/mon-ping.sh)
add_ceph_test(mon-scrub.sh ${CMAKE_SOURCE_DIR}/src/test/mon/mon-scrub.sh)
add_ceph_test(osd-crush.sh ${CMAKE_SOURCE_DIR}/src/test/mon/osd-crush.sh)
add_ceph_test(osd-erasure-code-profile.sh ${CMAKE_SOURCE_DIR}/src/test/mon/osd-erasure-code-profile.sh)
add_ceph_test(osd-pool-create.sh ${CMAKE_SOURCE_DIR}/src/test/mon/osd-pool-create.sh)

# unittest_mon_moncap
add_executable(unittest_mon_moncap EXCLUDE_FROM_ALL
  moncap.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_ceph_unittest(unittest_mon_moncap ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mon_moncap)
target_link_libraries(unittest_mon_moncap mon global)

# unittest_mon_pgmap
add_executable(unittest_mon_pgmap EXCLUDE_FROM_ALL
  PGMap.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_ceph_unittest(unittest_mon_pgmap ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mon_pgmap)
target_link_libraries(unittest_mon_pgmap mon global)

