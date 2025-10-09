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
