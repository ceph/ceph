#unittest_rgw_bencode
add_executable(unittest_rgw_bencode test_rgw_bencode.cc)
add_ceph_unittest(unittest_rgw_bencode ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_rgw_bencode)
target_link_libraries(unittest_rgw_bencode rgw_a)

#unitttest_rgw_period_history
add_executable(unittest_rgw_period_history test_rgw_period_history.cc)
add_ceph_unittest(unittest_rgw_period_history ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_rgw_period_history)
target_link_libraries(unittest_rgw_period_history rgw_a)

# unitttest_rgw_compression
add_executable(unittest_rgw_compression
  test_rgw_compression.cc
  $<TARGET_OBJECTS:unit-main>)
add_ceph_unittest(unittest_rgw_compression ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_rgw_compression)
target_link_libraries(unittest_rgw_compression rgw_a)

# unitttest_http_manager
add_executable(unittest_http_manager test_http_manager.cc)
add_ceph_unittest(unittest_http_manager ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_http_manager)
target_link_libraries(unittest_http_manager rgw_a)

set(test_rgw_a_src
  test_rgw_common.cc)
add_library(test_rgw_a STATIC ${test_rgw_a_src})

# ceph_test_rgw_manifest
set(test_rgw_manifest_srcs test_rgw_manifest.cc)
add_executable(ceph_test_rgw_manifest
  ${test_rgw_manifest_srcs}
  )
target_link_libraries(ceph_test_rgw_manifest
  rgw_a
  test_rgw_a
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
  ${BLKID_LIBRARIES}
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
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
  test_rgw_a
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
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  ${CRYPTO_LIBS}
  )
set_target_properties(ceph_test_rgw_obj PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# ceph_test_rgw_crypto
set(test_rgw_crypto_srcs test_rgw_crypto.cc)
add_executable(unittest_rgw_crypto
  ${test_rgw_crypto_srcs}
  )
add_ceph_unittest(unittest_rgw_crypto ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_rgw_crypto)

target_link_libraries(unittest_rgw_crypto
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
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  ${CRYPTO_LIBS}
  )
set_target_properties(unittest_rgw_crypto PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
