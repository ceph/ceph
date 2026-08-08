# cls_safety tests - tests for CLS method safety features like read/write policing

add_executable(ceph_test_cls_method_policing
  test_cls_method_policing.cc
  )
target_link_libraries(ceph_test_cls_method_policing
  librados
  global
  radostest
  radostest-cxx
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  )

install(TARGETS
  ceph_test_cls_method_policing
  DESTINATION ${CMAKE_INSTALL_BINDIR})