add_executable(ceph_test_cls_fifo
  test_cls_fifo.cc
  )
target_link_libraries(ceph_test_cls_fifo
  neorados_cls_fifo
  libneorados
  spawn
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  )
install(TARGETS
  ceph_test_cls_fifo
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_bench_cls_fifo
  bench_cls_fifo.cc
  )
target_link_libraries(ceph_bench_cls_fifo
  neorados_cls_fifo
  libneorados
  spawn
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )
install(TARGETS
  ceph_test_cls_fifo
  DESTINATION ${CMAKE_INSTALL_BINDIR})
