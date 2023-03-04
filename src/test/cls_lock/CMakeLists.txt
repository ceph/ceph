add_executable(ceph_test_cls_lock
  test_cls_lock.cc
  )
target_link_libraries(ceph_test_cls_lock
  cls_lock_client
  librados
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest-cxx
  )
install(TARGETS
  ceph_test_cls_lock
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neocls_lock
  test_neocls_lock.cc
  )
target_link_libraries(ceph_test_neocls_lock
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neocls_lock
  DESTINATION ${CMAKE_INSTALL_BINDIR})
