# ceph_test_cls_version
add_executable(ceph_test_cls_version
  test_cls_version.cc
  )
target_link_libraries(ceph_test_cls_version
  librados
  cls_version_client
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest-cxx
  )


add_executable(ceph_test_neocls_version
  test_neocls_version.cc
  )
target_link_libraries(ceph_test_neocls_version
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neocls_version
  DESTINATION ${CMAKE_INSTALL_BINDIR})
