# ceph_test_cls_statelog
add_executable(ceph_test_cls_statelog
  test_cls_statelog.cc
  )
target_link_libraries(ceph_test_cls_statelog
  librados
  cls_statelog_client
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

