add_executable(ceph_test_cls_fifo
  test_cls_fifo.cc
  )
target_link_libraries(ceph_test_cls_fifo
  cls_fifo_client
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
  ceph_test_cls_fifo
  DESTINATION ${CMAKE_INSTALL_BINDIR})
