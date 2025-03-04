# the crimson's backfill doesn't need nor use seastar
add_executable(unittest-crimson-backfill
  test_backfill.cc
  ${PROJECT_SOURCE_DIR}/src/auth/Crypto.cc
  ${PROJECT_SOURCE_DIR}/src/crimson/osd/backfill_state.cc
  ${PROJECT_SOURCE_DIR}/src/osd/recovery_types.cc)
add_ceph_unittest(unittest-crimson-backfill
  --memory 256M --smp 1)
target_link_libraries(unittest-crimson-backfill crimson GTest::Main)

add_executable(unittest-seastar-buffer
  test_buffer.cc)
add_ceph_unittest(unittest-seastar-buffer
  --memory 256M --smp 1)
target_link_libraries(unittest-seastar-buffer crimson)

add_executable(unittest-seastar-denc
  test_denc.cc)
add_ceph_unittest(unittest-seastar-denc --memory 256M --smp 1)
target_link_libraries(unittest-seastar-denc crimson GTest::Main)

add_executable(unittest-seastar-socket test_socket.cc)
add_ceph_unittest(unittest-seastar-socket
  --memory 256M --smp 4)
target_link_libraries(unittest-seastar-socket crimson)

add_executable(unittest-seastar-messenger test_messenger.cc)
add_ceph_unittest(unittest-seastar-messenger
  --memory 256M --smp 4)
target_link_libraries(unittest-seastar-messenger crimson)

add_executable(test-seastar-messenger-peer test_messenger_peer.cc)
target_link_libraries(test-seastar-messenger-peer ceph-common global ${ALLOC_LIBS})

add_executable(test-seastar-echo
  test_alien_echo.cc)
target_link_libraries(test-seastar-echo crimson)

add_executable(test-async-echo
  test_async_echo.cc)
target_link_libraries(test-async-echo ceph-common global)

add_executable(unittest-seastar-alienstore-thread-pool
  test_alienstore_thread_pool.cc
  ${PROJECT_SOURCE_DIR}/src/crimson/osd/lsan_suppressions.cc)
add_ceph_unittest(unittest-seastar-alienstore-thread-pool
  --memory 256M --smp 1)
target_link_libraries(unittest-seastar-alienstore-thread-pool
  crimson-alienstore
  crimson)

add_executable(unittest-seastar-config
  test_config.cc)
add_ceph_unittest(unittest-seastar-config
  --memory 256M --smp 4)
target_link_libraries(unittest-seastar-config crimson)

add_executable(unittest-seastar-monc
  test_monc.cc)
target_link_libraries(unittest-seastar-monc crimson)

add_executable(unittest-seastar-perfcounters
  test_perfcounters.cc)
add_ceph_unittest(unittest-seastar-perfcounters
  --memory 256M --smp 1)
target_link_libraries(unittest-seastar-perfcounters crimson)

add_executable(unittest-seastar-lru
  test_lru.cc)
add_ceph_unittest(unittest-seastar-lru
  --memory 256M --smp 1)
target_link_libraries(unittest-seastar-lru crimson GTest::Main)

add_executable(unittest-seastar-calc-subsets
    ${PROJECT_SOURCE_DIR}/src/crimson/osd/object_metadata_helper.cc
  test_calc_subsets.cc)
add_ceph_unittest(unittest-seastar-calc-subsets
  --memory 256M --smp 1)
target_link_libraries(unittest-seastar-calc-subsets crimson GTest::Main)

add_executable(unittest-fixed-kv-node-layout
  test_fixed_kv_node_layout.cc)
add_ceph_unittest(unittest-fixed-kv-node-layout)

add_executable(unittest-interruptible-future
  test_interruptible_future.cc
  gtest_seastar.cc)
add_ceph_unittest(unittest-interruptible-future
  --memory 256M --smp 1)
target_link_libraries(
  unittest-interruptible-future
  crimson-common)

add_executable(unittest-seastar-messenger-thrash test_messenger_thrash.cc)
add_ceph_unittest(unittest-seastar-messenger-thrash
  --memory 256M --smp 1)
target_link_libraries(unittest-seastar-messenger-thrash crimson)

add_subdirectory(seastore)

add_library(crimson-gtest STATIC
  gtest_seastar.cc)
target_link_libraries(crimson-gtest crimson-common GTest::GTest)
add_library(crimson::gtest ALIAS crimson-gtest)

add_executable(unittest-seastar-errorator
  test_errorator.cc)
target_link_libraries(
  unittest-seastar-errorator
  crimson::gtest)
add_ceph_unittest(unittest-seastar-errorator
  --memory 256M --smp 1)

add_executable(unittest-crimson-coroutine
  test_crimson_coroutine.cc)
target_link_libraries(
  unittest-crimson-coroutine
  crimson::gtest)
add_ceph_unittest(unittest-crimson-coroutine
  --memory 256M --smp 1)

add_executable(unittest-crimson-scrub
  test_crimson_scrub.cc
  ${PROJECT_SOURCE_DIR}/src/crimson/osd/scrub/scrub_machine.cc
  ${PROJECT_SOURCE_DIR}/src/crimson/osd/scrub/scrub_validator.cc
  ${PROJECT_SOURCE_DIR}/src/osd/ECUtil.cc)
target_link_libraries(
  unittest-crimson-scrub
  crimson-common
  crimson::gtest)
