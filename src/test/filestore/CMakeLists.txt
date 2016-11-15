#ceph_test_filestore
add_executable(ceph_test_filestore 
  TestFileStore.cc)
set_target_properties(ceph_test_filestore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_filestore os global ${UNITTEST_LIBS})


