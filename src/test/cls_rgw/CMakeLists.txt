add_executable(ceph_test_cls_rgw
  test_cls_rgw.cc
  )
target_link_libraries(ceph_test_cls_rgw
  cls_rgw_client
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx)
install(TARGETS
  ceph_test_cls_rgw
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_cls_rgw_stats test_cls_rgw_stats.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(ceph_test_cls_rgw_stats cls_rgw_client global
  librados ${UNITTEST_LIBS} radostest-cxx)
install(TARGETS ceph_test_cls_rgw_stats DESTINATION ${CMAKE_INSTALL_BINDIR})
