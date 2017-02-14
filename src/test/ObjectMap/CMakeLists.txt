# ceph_test_object_map
add_executable(ceph_test_object_map
  test_object_map.cc
  KeyValueDBMemory.cc
  )
set_target_properties(ceph_test_object_map PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
add_ceph_unittest(ceph_test_object_map ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph_test_object_map)
target_link_libraries(ceph_test_object_map
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_keyvaluedb_atomicity
add_executable(ceph_test_keyvaluedb_atomicity
  test_keyvaluedb_atomicity.cc
  )
set_target_properties(ceph_test_keyvaluedb_atomicity PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_keyvaluedb_atomicity
  os
  common
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
set_target_properties(ceph_test_keyvaluedb_iterators PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_keyvaluedb_iterators
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

