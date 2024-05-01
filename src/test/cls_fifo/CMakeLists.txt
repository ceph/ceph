add_executable(ceph_test_neocls_fifo
  ceph_test_neocls_fifo.cc
  )
target_link_libraries(ceph_test_neocls_fifo
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neocls_fifo
  DESTINATION ${CMAKE_INSTALL_BINDIR})
