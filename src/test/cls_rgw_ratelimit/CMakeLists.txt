add_executable(ceph_test_cls_rgw_ratelimit
  test_cls_rgw_ratelimit.cc)
target_link_libraries(ceph_test_cls_rgw_ratelimit
  cls_rgw_ratelimit_client
  librados
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS})
install(TARGETS ceph_test_cls_rgw_ratelimit DESTINATION ${CMAKE_INSTALL_BINDIR})
add_ceph_test(ceph_test_cls_rgw_ratelimit ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph_test_cls_rgw_ratelimit)
