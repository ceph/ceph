add_executable(ceph_test_cls_blake3 test_cls_blake3.cc)
target_link_libraries(ceph_test_cls_blake3 cls_blake3_client librados radostest-cxx ${UNITTEST_LIBS} BLAKE3::blake3)
install(TARGETS ceph_test_cls_blake3 DESTINATION ${CMAKE_INSTALL_BINDIR})
