add_executable(ceph_test_objectcacher_stress
  object_cacher_stress.cc
  FakeWriteback.cc
  MemWriteback.cc
  )
target_link_libraries(ceph_test_objectcacher_stress
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_objectcacher_stress
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_objectcacher_misc
  object_cacher_misc.cc
  MemWriteback.cc
  )
target_link_libraries(ceph_test_objectcacher_misc
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_objectcacher_misc
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# unittest_journaler_lock — regression for Journaler lock held across Objecter throttle
add_executable(unittest_journaler_lock
  test_Journaler_lock.cc
  ${CMAKE_SOURCE_DIR}/src/osdc/Journaler.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_journaler_lock)
target_link_libraries(unittest_journaler_lock
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
