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

add_executable(ceph_test_neocls_log
  test_neocls_log.cc
  )
target_link_libraries(ceph_test_neocls_log
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neocls_log
  DESTINATION ${CMAKE_INSTALL_BINDIR})
