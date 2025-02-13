add_executable(ceph_test_cls_sdk
  test_cls_sdk.cc
  )
target_link_libraries(ceph_test_cls_sdk
  librados
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_cls_sdk
  DESTINATION ${CMAKE_INSTALL_BINDIR})
