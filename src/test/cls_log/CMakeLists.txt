add_executable(ceph_test_cls_log
  test_cls_log.cc)
target_link_libraries(ceph_test_cls_log
  librados
  cls_log_client
  global
  radostest-cxx
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )
install(TARGETS
  ceph_test_cls_log
  DESTINATION ${CMAKE_INSTALL_BINDIR})
