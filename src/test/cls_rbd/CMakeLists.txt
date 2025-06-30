# cls_test_cls_rbd
add_executable(ceph_test_cls_rbd
  test_cls_rbd.cc
  $<TARGET_OBJECTS:common_texttable_obj>)
target_link_libraries(ceph_test_cls_rbd
  cls_rbd_client
  cls_lock_client
  librados
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest-cxx)
install(TARGETS
  ceph_test_cls_rbd
  DESTINATION ${CMAKE_INSTALL_BINDIR})
