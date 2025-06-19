add_executable(ceph_test_cls_sem_set
  test_cls_sem_set.cc
  )
target_link_libraries(ceph_test_cls_sem_set
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_cls_sem_set
  DESTINATION ${CMAKE_INSTALL_BINDIR})
