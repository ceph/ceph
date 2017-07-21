add_executable(ceph_test_cls_sdk
  test_cls_sdk.cc
  )
set_target_properties(ceph_test_cls_sdk PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_sdk
  librados
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_cls_sdk
  DESTINATION ${CMAKE_INSTALL_BINDIR})
