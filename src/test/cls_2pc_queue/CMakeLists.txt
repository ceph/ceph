add_executable(ceph_test_cls_2pc_queue
  test_cls_2pc_queue.cc
)
target_link_libraries(ceph_test_cls_2pc_queue
  cls_2pc_queue_client
  cls_queue_client
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx)

 install(TARGETS
  ceph_test_cls_2pc_queue
  DESTINATION ${CMAKE_INSTALL_BINDIR})

