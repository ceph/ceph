add_executable(ceph_test_cls_cas
  test_cls_cas.cc)
target_link_libraries(ceph_test_cls_cas
  librados
  cls_cas_client
  global
  radostest-cxx
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )
install(TARGETS
  ceph_test_cls_cas
  DESTINATION ${CMAKE_INSTALL_BINDIR})
