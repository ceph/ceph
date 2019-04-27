add_executable(ceph_test_cls_hello
  test_cls_hello.cc
  )
target_link_libraries(ceph_test_cls_hello
  librados
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_cls_hello
  DESTINATION ${CMAKE_INSTALL_BINDIR})
