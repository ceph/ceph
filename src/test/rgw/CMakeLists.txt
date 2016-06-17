#unitttest_rgw_period_history
add_executable(unittest_rgw_period_history EXCLUDE_FROM_ALL test_rgw_period_history.cc)
add_ceph_unittest(unittest_rgw_period_history ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_rgw_period_history)
target_link_libraries(unittest_rgw_period_history rgw_a)

# unitttest_http_manager
add_executable(unittest_http_manager EXCLUDE_FROM_ALL test_http_manager.cc)
add_ceph_unittest(unittest_http_manager ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_http_manager)
target_link_libraries(unittest_http_manager rgw_a)

# ceph_test_rgw_manifest
set(test_rgw_manifest_srcs test_rgw_manifest.cc)
add_executable(ceph_test_rgw_manifest
  ${test_rgw_manifest_srcs}
  )
target_link_libraries(ceph_test_rgw_manifest
  rgw_a
  cls_rgw_client
  cls_lock_client
  cls_refcount_client
  cls_log_client
  cls_statelog_client
  cls_timeindex_client
  cls_version_client
  cls_replica_log_client
  cls_user_client
  librados
  global
  curl
  expat
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  ${CRYPTO_LIBS}
  )
set_target_properties(ceph_test_rgw_manifest PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# ceph_test_rgw_obj
set(test_rgw_obj_srcs test_rgw_obj.cc)
add_executable(ceph_test_rgw_obj
  ${test_rgw_obj_srcs}
  )
target_link_libraries(ceph_test_rgw_obj
  rgw_a
  cls_rgw_client
  cls_lock_client
  cls_refcount_client
  cls_log_client
  cls_statelog_client
  cls_version_client
  cls_replica_log_client
  cls_user_client
  librados
  global
  curl
  uuid
  expat
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  ${CRYPTO_LIBS}
  )
set_target_properties(ceph_test_rgw_obj PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

