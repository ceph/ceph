add_executable(ceph_test_cls_cmpxattr test_cls_cmpxattr.cc)
target_link_libraries(ceph_test_cls_cmpxattr cls_cmpxattr_client librados radostest-cxx ${UNITTEST_LIBS})
install(TARGETS ceph_test_cls_cmpxattr DESTINATION ${CMAKE_INSTALL_BINDIR})
