add_executable(ceph_test_cls_timeindex
  test_cls_timeindex.cc
  )
target_link_libraries(ceph_test_cls_timeindex
  cls_timeindex_client
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
  ceph_test_cls_timeindex
  DESTINATION ${CMAKE_INSTALL_BINDIR})
