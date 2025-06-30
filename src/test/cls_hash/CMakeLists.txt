add_executable(ceph_test_cls_hash test_cls_hash.cc)
target_link_libraries(ceph_test_cls_hash cls_hash_client librados radostest-cxx ${UNITTEST_LIBS} BLAKE3::blake3)
install(TARGETS ceph_test_cls_hash DESTINATION ${CMAKE_INSTALL_BINDIR})
