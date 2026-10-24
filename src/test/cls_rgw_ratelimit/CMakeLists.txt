add_executable(ceph_test_cls_rgw_ratelimit
  test_cls_rgw_ratelimit.cc
  ${CMAKE_SOURCE_DIR}/src/rgw/rgw_ratelimit_core.cc)
target_include_directories(ceph_test_cls_rgw_ratelimit PRIVATE
  ${CMAKE_SOURCE_DIR}/src/rgw)
target_link_libraries(ceph_test_cls_rgw_ratelimit
  cls_rgw_ratelimit_client
  librados
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  radostest-cxx)
install(TARGETS ceph_test_cls_rgw_ratelimit DESTINATION ${CMAKE_INSTALL_BINDIR})
add_ceph_test(ceph_test_cls_rgw_ratelimit ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ceph_test_cls_rgw_ratelimit)
