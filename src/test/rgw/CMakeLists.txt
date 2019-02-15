#unittest_rgw_bencode
add_executable(unittest_rgw_bencode test_rgw_bencode.cc)
add_ceph_unittest(unittest_rgw_bencode)
target_link_libraries(unittest_rgw_bencode rgw_a)

#unitttest_rgw_period_history
add_executable(unittest_rgw_period_history test_rgw_period_history.cc)
add_ceph_unittest(unittest_rgw_period_history)
target_link_libraries(unittest_rgw_period_history rgw_a)

# unitttest_rgw_compression
add_executable(unittest_rgw_compression
  test_rgw_compression.cc
  $<TARGET_OBJECTS:unit-main>)
add_ceph_unittest(unittest_rgw_compression)
target_link_libraries(unittest_rgw_compression rgw_a)

# unitttest_http_manager
add_executable(unittest_http_manager test_http_manager.cc)
add_ceph_unittest(unittest_http_manager)
target_link_libraries(unittest_http_manager rgw_a)

# unitttest_rgw_reshard_wait
add_executable(unittest_rgw_reshard_wait test_rgw_reshard_wait.cc)
add_ceph_unittest(unittest_rgw_reshard_wait)
target_link_libraries(unittest_rgw_reshard_wait rgw_a)

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
  cls_timeindex_client
  cls_version_client
  cls_user_client
  librados
  global
  ${BLKID_LIBRARIES}
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  ${CRYPTO_LIBS})

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
  cls_version_client
  cls_user_client
  librados
  global
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  ${CRYPTO_LIBS}
  )

set(test_rgw_crypto_srcs test_rgw_crypto.cc)
add_executable(unittest_rgw_crypto
  ${test_rgw_crypto_srcs}
  )
add_ceph_unittest(unittest_rgw_crypto)
target_link_libraries(unittest_rgw_crypto
  rgw_a
  cls_rgw_client
  cls_lock_client
  cls_refcount_client
  cls_log_client
  cls_version_client
  cls_user_client
  librados
  global
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  ${CRYPTO_LIBS}
  )

add_executable(unittest_rgw_putobj test_rgw_putobj.cc)
add_ceph_unittest(unittest_rgw_putobj)
target_link_libraries(unittest_rgw_putobj rgw_a ${UNITTEST_LIBS})

add_executable(ceph_test_rgw_throttle
  test_rgw_throttle.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(ceph_test_rgw_throttle rgw_a
  librados global ${UNITTEST_LIBS})

add_executable(unittest_rgw_iam_policy test_rgw_iam_policy.cc)
add_ceph_unittest(unittest_rgw_iam_policy)
target_link_libraries(unittest_rgw_iam_policy
  rgw_a
  cls_rgw_client
  cls_lock_client
  cls_refcount_client
  cls_log_client
  cls_version_client
  cls_user_client
  librados
  global
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  ${CRYPTO_LIBS}
  )

add_executable(unittest_rgw_string test_rgw_string.cc)
add_ceph_unittest(unittest_rgw_string)

# unitttest_rgw_dmclock_queue
add_executable(unittest_rgw_dmclock_scheduler test_rgw_dmclock_scheduler.cc $<TARGET_OBJECTS:unit-main>)
add_ceph_unittest(unittest_rgw_dmclock_scheduler)
target_link_libraries(unittest_rgw_dmclock_scheduler rgw_a dmclock ${Boost_LIBRARIES})
if(WITH_BOOST_CONTEXT)
  target_compile_definitions(unittest_rgw_dmclock_scheduler PRIVATE BOOST_COROUTINES_NO_DEPRECATION_WARNING)
  target_link_libraries(unittest_rgw_dmclock_scheduler Boost::coroutine Boost::context)
endif()
