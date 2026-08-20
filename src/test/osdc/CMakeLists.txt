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

# unittest_readrdma_encoding
add_executable(unittest_readrdma_encoding
  test_readrdma_encoding.cc
  )
add_ceph_unittest(unittest_readrdma_encoding)
target_link_libraries(unittest_readrdma_encoding osdc global ceph-common)

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
