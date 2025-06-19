# ceph_test_object_map
add_executable(ceph_test_object_map
  KeyValueDBMemory.cc
  )
add_ceph_unittest(ceph_test_object_map)
target_link_libraries(ceph_test_object_map
  os
  ceph-common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_keyvaluedb_atomicity
add_executable(ceph_test_keyvaluedb_atomicity
  test_keyvaluedb_atomicity.cc
  )
target_link_libraries(ceph_test_keyvaluedb_atomicity
  os
  ceph-common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_keyvaluedb_iterators
add_executable(ceph_test_keyvaluedb_iterators
  test_keyvaluedb_iterators.cc
  KeyValueDBMemory.cc
  )
target_link_libraries(ceph_test_keyvaluedb_iterators
  os
  ceph-common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

