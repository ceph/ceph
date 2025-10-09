# ceph_test_cls_refcount
add_executable(ceph_test_cls_refcount
  test_cls_refcount.cc
  )
target_link_libraries(ceph_test_cls_refcount
  librados
  cls_refcount_client
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest-cxx
  )
install(TARGETS
  ceph_test_cls_refcount
  DESTINATION ${CMAKE_INSTALL_BINDIR})
