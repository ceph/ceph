add_executable(ceph_test_cls_queue
  test_cls_queue.cc
)
target_link_libraries(ceph_test_cls_queue
  cls_queue_client
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx)

install(TARGETS
  ceph_test_cls_queue
  DESTINATION ${CMAKE_INSTALL_BINDIR})

