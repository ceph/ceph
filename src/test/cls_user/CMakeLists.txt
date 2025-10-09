add_executable(ceph_test_cls_user test_cls_user.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(ceph_test_cls_user cls_user_client global
  librados ${UNITTEST_LIBS} radostest-cxx)
install(TARGETS ceph_test_cls_user DESTINATION ${CMAKE_INSTALL_BINDIR})
