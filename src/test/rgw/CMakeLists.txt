
if(WITH_RADOSGW_AMQP_ENDPOINT)
  # amqp mock library
  set(amqp_mock_src
    amqp_mock.cc amqp_url.c)
  add_library(amqp_mock STATIC ${amqp_mock_src})
endif()

if(WITH_RADOSGW_KAFKA_ENDPOINT)
  # kafka stub library
  set(kafka_stub_src
    kafka_stub.cc)
  add_library(kafka_stub STATIC ${kafka_stub_src})
endif()

#unittest_rgw_bencode
add_executable(unittest_rgw_bencode test_rgw_bencode.cc)
add_ceph_unittest(unittest_rgw_bencode)
target_link_libraries(unittest_rgw_bencode ${rgw_libs})

# unittest_rgw_bucket_sync_cache
add_executable(unittest_rgw_bucket_sync_cache test_rgw_bucket_sync_cache.cc)
add_ceph_unittest(unittest_rgw_bucket_sync_cache)
target_link_libraries(unittest_rgw_bucket_sync_cache ${rgw_libs})

#unitttest_rgw_period_history
add_executable(unittest_rgw_period_history test_rgw_period_history.cc)
add_ceph_unittest(unittest_rgw_period_history)
target_link_libraries(unittest_rgw_period_history ${rgw_libs})

# unitttest_rgw_compression
add_executable(unittest_rgw_compression
  test_rgw_compression.cc
  $<TARGET_OBJECTS:unit-main>)
add_ceph_unittest(unittest_rgw_compression)
target_link_libraries(unittest_rgw_compression ${rgw_libs})

# unitttest_http_manager
add_executable(unittest_http_manager test_http_manager.cc)
add_ceph_unittest(unittest_http_manager)
target_link_libraries(unittest_http_manager ${rgw_libs})

# unitttest_rgw_reshard_wait
add_executable(unittest_rgw_reshard_wait test_rgw_reshard_wait.cc)
add_ceph_unittest(unittest_rgw_reshard_wait)
target_link_libraries(unittest_rgw_reshard_wait ${rgw_libs})

set(test_rgw_a_src test_rgw_common.cc)
add_library(test_rgw_a STATIC ${test_rgw_a_src})
target_link_libraries(test_rgw_a ${rgw_libs})

# ceph_test_rgw_manifest
set(test_rgw_manifest_srcs test_rgw_manifest.cc)
add_executable(ceph_test_rgw_manifest
  ${test_rgw_manifest_srcs}
  )
target_link_libraries(ceph_test_rgw_manifest
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
  test_rgw_a
  cls_rgw_client
  cls_lock_client
  cls_refcount_client
  cls_log_client
  cls_version_client
  cls_user_client
  librados
  global
  ceph-common
  ${CURL_LIBRARIES}
  ${EXPAT_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  ${CRYPTO_LIBS}
  )
install(TARGETS ceph_test_rgw_obj DESTINATION ${CMAKE_INSTALL_BINDIR})

set(test_rgw_crypto_srcs test_rgw_crypto.cc)
add_executable(unittest_rgw_crypto
  ${test_rgw_crypto_srcs}
  )
add_ceph_unittest(unittest_rgw_crypto)
target_link_libraries(unittest_rgw_crypto
  ${rgw_libs}
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

set(test_rgw_reshard_srcs test_rgw_reshard.cc)
add_executable(unittest_rgw_reshard
  ${test_rgw_reshard_srcs}
  )
add_ceph_unittest(unittest_rgw_reshard)
target_link_libraries(unittest_rgw_reshard
  ${rgw_libs}
  )

add_executable(unittest_rgw_putobj test_rgw_putobj.cc)
add_ceph_unittest(unittest_rgw_putobj)
target_link_libraries(unittest_rgw_putobj ${rgw_libs} ${UNITTEST_LIBS})

add_executable(ceph_test_rgw_throttle
  test_rgw_throttle.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(ceph_test_rgw_throttle ${rgw_libs}
  librados global ${UNITTEST_LIBS})
install(TARGETS ceph_test_rgw_throttle DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(unittest_rgw_iam_policy test_rgw_iam_policy.cc)
add_ceph_unittest(unittest_rgw_iam_policy)
target_link_libraries(unittest_rgw_iam_policy
  ${rgw_libs}
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

target_link_libraries(unittest_rgw_dmclock_scheduler rgw_schedulers global ${UNITTEST_LIBS})
if(WITH_BOOST_CONTEXT)
  target_compile_definitions(unittest_rgw_dmclock_scheduler PRIVATE BOOST_COROUTINES_NO_DEPRECATION_WARNING)
  target_link_libraries(unittest_rgw_dmclock_scheduler Boost::coroutine Boost::context)
endif()

if(WITH_RADOSGW_AMQP_ENDPOINT)
  add_executable(unittest_rgw_amqp test_rgw_amqp.cc)
  add_ceph_unittest(unittest_rgw_amqp)
  target_link_libraries(unittest_rgw_amqp ${rgw_libs})
endif()

# unittest_rgw_xml
add_executable(unittest_rgw_xml test_rgw_xml.cc)
add_ceph_unittest(unittest_rgw_xml)

target_link_libraries(unittest_rgw_xml ${rgw_libs} ${EXPAT_LIBRARIES})

# unittest_rgw_arn
add_executable(unittest_rgw_arn test_rgw_arn.cc)
add_ceph_unittest(unittest_rgw_arn)

target_link_libraries(unittest_rgw_arn ${rgw_libs})

# unittest_rgw_kms
add_executable(unittest_rgw_kms test_rgw_kms.cc)
add_ceph_unittest(unittest_rgw_kms)

target_link_libraries(unittest_rgw_kms ${rgw_libs})

# unittest_rgw_url
add_executable(unittest_rgw_url test_rgw_url.cc)
add_ceph_unittest(unittest_rgw_url)

target_link_libraries(unittest_rgw_url ${rgw_libs})

add_executable(ceph_test_rgw_gc_log test_rgw_gc_log.cc $<TARGET_OBJECTS:unit-main>)
target_link_libraries(ceph_test_rgw_gc_log ${rgw_libs} radostest-cxx)
install(TARGETS ceph_test_rgw_gc_log DESTINATION ${CMAKE_INSTALL_BINDIR})

add_ceph_test(test-ceph-diff-sorted.sh
  ${CMAKE_CURRENT_SOURCE_DIR}/test-ceph-diff-sorted.sh)
