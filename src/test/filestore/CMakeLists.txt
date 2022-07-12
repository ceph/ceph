#ceph_test_filestore
add_executable(ceph_test_filestore 
  TestFileStore.cc)
target_link_libraries(ceph_test_filestore os global ${UNITTEST_LIBS})


