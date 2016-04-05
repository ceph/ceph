# test_timers
add_executable(test_timers
  TestTimers.cc
  )
target_link_libraries(test_timers global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# test_signal_handlers
add_executable(test_signal_handlers
  TestSignalHandlers.cc
  )
target_link_libraries(test_signal_handlers global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# test_crypt
add_executable(test_crypto
  testcrypto.cc
  )
target_link_libraries(test_crypto
  global
  ${CRYPTO_LIBS}
  m
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_build_libcommon buildtest_skeleton.cc)
target_link_libraries(test_build_libcommon common pthread ${CRYPTO_LIBS} ${EXTRALIBS})

add_executable(test_build_librgw buildtest_skeleton.cc)
target_link_libraries(test_build_librgw rgw_a global pthread ${CRYPTO_LIBS} ${EXTRALIBS})

# From src/test/Makefile-client.am: I dont get this one... testing the osdc build but link in libcephfs?
add_executable(test_build_libcephfs buildtest_skeleton.cc)
target_link_libraries(test_build_libcephfs cephfs expat pthread ${CRYPTO_LIBS} ${EXTRALIBS})

add_executable(test_build_librados buildtest_skeleton.cc)
target_link_libraries(test_build_librados librados pthread ${CRYPTO_LIBS} ${EXTRALIBS} osdc osd os common cls_lock_client ${BLKID_LIBRARIES})

# test_rados
add_executable(test_rados 
  osd/TestRados.cc
  osd/TestOpStat.cc
  osd/Object.cc osd/RadosModel.cc
  )
target_link_libraries(test_rados
  librados
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# test_mutate
add_executable(test_mutate
  test_mutate.cc
  )
target_link_libraries(test_mutate global librados ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS})

# test_rewrite_latency
add_executable(test_rewrite_latency
  test_rewrite_latency.cc
  )
target_link_libraries(test_rewrite_latency common
  ${CMAKE_DL_LIBS}
  ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})

# test_trans
add_executable(test_trans
  test_trans.cc
  )
target_link_libraries(test_trans os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# test_keys
add_executable(test_keys
  testkeys.cc
  )
target_link_libraries(test_keys mon global ${CMAKE_DL_LIBS})

# get_command_descriptions
add_executable(get_command_descriptions
  common/get_command_descriptions.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  )
target_link_libraries(get_command_descriptions
  mon
  global
  leveldb
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

## Benchmarks

# smalliobench
set(smalliobench_srcs
  bench/small_io_bench.cc
  bench/rados_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(smalliobench
  ${smalliobench_srcs}
  )
target_link_libraries(smalliobench librados ${Boost_PROGRAM_OPTIONS_LIBRARY} global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# smalliobenchfs
set(smalliobenchfs_srcs
  bench/small_io_bench_fs.cc
  bench/testfilestore_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(smalliobenchfs
  ${smalliobenchfs_srcs}
  )
target_link_libraries(smalliobenchfs librados ${Boost_PROGRAM_OPTIONS_LIBRARY} os global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# smalliobenchdumb
set(smalliobenchdumb_srcs
  bench/small_io_bench_dumb.cc
  bench/dumb_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(smalliobenchdumb
  ${smalliobenchdumb_srcs}
  )
target_link_libraries(smalliobenchdumb librados ${Boost_PROGRAM_OPTIONS_LIBRARY} os global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# smalliobenchrbd
if (${WITH_RBD})
  set(smalliobenchrbd_srcs
    bench/small_io_bench_rbd.cc
    bench/rbd_backend.cc
    bench/detailed_stat_collector.cc
    bench/bencher.cc
    ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
    ${CMAKE_SOURCE_DIR}/src/common/secret.c
  )
  add_executable(smalliobenchrbd
    ${smalliobenchrbd_srcs}
    )
  target_link_libraries(smalliobenchrbd
    librbd
    librados
    os
    global
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
    udev
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    keyutils
    )
endif (${WITH_RBD})

# tpbench
set(tpbench_srcs
  bench/tp_bench.cc
  bench/detailed_stat_collector.cc)
add_executable(tpbench
  ${tpbench_srcs}
  )
target_link_libraries(tpbench librados ${Boost_PROGRAM_OPTIONS_LIBRARY} global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# omapbench
set(omapbench_srcs
  omap_bench.cc
  )
add_executable(omapbench
  ${omapbench_srcs}
  )
target_link_libraries(omapbench
  librados
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

# kvstorebench
set(kvstorebench_srcs
  kv_store_bench.cc
  ${CMAKE_SOURCE_DIR}/src/key_value_store/kv_flat_btree_async.cc
  )
add_executable(kvstorebench
  ${kvstorebench_srcs}
  )
target_link_libraries(kvstorebench librados global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# ceph_objectstore_bench
add_executable(ceph_objectstore_bench objectstore_bench.cc)
target_link_libraries(ceph_objectstore_bench global ${BLKID_LIBRARIES} os)

## System tests

# systest
set(libsystest_srcs system/cross_process_sem.cc
  system/systest_runnable.cc
  system/systest_settings.cc
  system/st_rados_create_pool.cc
  system/st_rados_delete_pool.cc
  system/st_rados_list_objects.cc
  system/st_rados_watch.cc
  system/st_rados_notify.cc)
add_library(systest STATIC ${libsystest_srcs})

# test_rados_list_parallel
add_executable(test_rados_list_parallel
  system/rados_list_parallel.cc
  )
target_link_libraries(test_rados_list_parallel librados systest global pthread
  rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# test_rados_open_pools_parallel
set(test_rados_open_pools_parallel_srcs system/rados_open_pools_parallel.cc)
add_executable(test_rados_open_pools_parallel
  ${test_rados_open_pools_parallel_srcs}
  )
target_link_libraries(test_rados_open_pools_parallel librados systest global
  pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# test_rados_delete_pools_parallel
set(test_rados_delete_pools_parallel_srcs
  system/rados_delete_pools_parallel.cc
  system/st_rados_create_pool.cc
  system/st_rados_delete_pool.cc
  system/st_rados_list_objects.cc

  )
add_executable(test_rados_delete_pools_parallel
  ${test_rados_delete_pools_parallel_srcs}
  )
target_link_libraries(test_rados_delete_pools_parallel librados systest global
  pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# test_rados_watch_notify
set(test_rados_watch_notify_srcs
  system/rados_watch_notify.cc
  system/st_rados_create_pool.cc
  system/st_rados_delete_pool.cc
  system/st_rados_delete_objs.cc
  system/st_rados_watch.cc
  system/st_rados_notify.cc
  )
add_executable(test_rados_watch_notify
  ${test_rados_watch_notify_srcs}
  )
target_link_libraries(test_rados_watch_notify librados systest global
  pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# bench_log
set(bench_log_srcs
  bench_log.cc
  )
add_executable(bench_log
  ${bench_log_srcs}
  )
target_link_libraries(bench_log global pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

## Unit tests
#make check starts here
set(UNITTEST_LIBS gmock_main gmock gtest ${PTHREAD_LIBS})
set(UNITTEST_CXX_FLAGS "-I${CMAKE_SOURCE_DIR}/src/gmock/include -I${CMAKE_BINARY_DIR}/src/gmock/include -I${CMAKE_SOURCE_DIR}/src/gmock/gtest/include -I${CMAKE_BINARY_DIR}/src/gmock/gtest/include -fno-strict-aliasing")

#following dependencies are run inside make check unit tests
add_dependencies(check 
  ceph-mon
  ceph
  ceph-authtool
  get_command_descriptions
  crushtool
  ceph-conf
  rados
  ceph-mds
  monmaptool
  ceph-osd
  ceph-dencoder
  ceph-objectstore-tool)

add_test(NAME test_helpers COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/test-ceph-helpers.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check test_helpers)

add_test(NAME osd_config COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/osd/osd-config.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check osd_config)

add_test(NAME osd_copy_from COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/osd/osd-copy-from.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check osd_copy_from)

add_test(NAME osd_crush COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/osd-crush.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check osd_crush)

add_test(NAME ceph_disk_tox COMMAND bash ${CMAKE_SOURCE_DIR}/src/ceph-disk/run-tox.sh)
add_dependencies(check ceph_disk_tox)

add_test(NAME rados_striper COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/libradosstriper/rados-striper.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check rados_striper)

add_test(NAME osd_bench COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/osd/osd-bench.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check osd_bench)

add_test(NAME test_erasure_code COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/erasure-code/test-erasure-code.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check test_erasure_code)

add_test(NAME test_erasure_eio COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/erasure-code/test-erasure-eio.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check test_erasure_eio)

#osd_scrub_repair is hanging for unknown reasons
#add_test(NAME osd_scrub_repair COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/osd/osd-scrub-repair.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
#add_dependencies(check osd_scrub_repair)

add_test(NAME cephtool_test_rados COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/cephtool-test-rados.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check cephtool_test_rados)

add_test(NAME cephtool_test_mon COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/cephtool-test-mon.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check cephtool_test_mon)

add_test(NAME ceph_objectstore_tool_py COMMAND python ${CMAKE_SOURCE_DIR}/src/test/ceph_objectstore_tool.py WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check ceph_objectstore_tool_py)

add_test(NAME osd_mark_down COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/osd/osd-markdown.sh)
add_dependencies(check osd-markdown)

add_test(NAME ceph_disk COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/ceph-disk.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check ceph_disk)

# unittest_admin_socket
add_executable(unittest_admin_socket EXCLUDE_FROM_ALL
  admin_socket.cc
  )
add_test(unittest_admin_socket unittest_admin_socket)
add_dependencies(check unittest_admin_socket)
target_link_libraries(unittest_admin_socket global ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_admin_socket PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_blkdev
add_executable(unittest_blkdev EXCLUDE_FROM_ALL
  common/test_blkdev.cc
  )
add_test(NAME unittest_blkdev COMMAND ./unittest_blkdev WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check unittest_blkdev)
target_link_libraries(unittest_blkdev
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_blkdev PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_chain_xattr
add_executable(unittest_chain_xattr EXCLUDE_FROM_ALL
  objectstore/chain_xattr.cc
  )
add_test(unittest_chain_xattr unittest_chain_xattr) 
add_dependencies(check unittest_chain_xattr)
target_link_libraries(unittest_chain_xattr
  os
  global
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_chain_xattr PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_test(NAME mkfs COMMAND bash	${CMAKE_SOURCE_DIR}/src/test/mon/mkfs.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check mkfs)

add_test(NAME misc COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/misc.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check misc)

add_test(NAME mon_created_time COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/mon-created-time.sh)
add_dependencies(check mon_created_time)

add_test(NAME test_ceph_daemon COMMAND python ${CMAKE_SOURCE_DIR}/src/test/pybind/test_ceph_daemon.py WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/test/pybind)
add_dependencies(check test_ceph_daemon)

set(CTEST_ENVIROMENT "PYTHONPATH=$PYTHONPATH:${CMAKE_SOURCE_DIR}/src/pybind")
add_test(NAME test_ceph_argparse_py COMMAND python ${CMAKE_SOURCE_DIR}/src/test/pybind/test_ceph_argparse.py WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/src/test)
add_dependencies(check test_ceph_argparse_py)

add_test(NAME unittest_bufferlist_shell COMMAND bash ${CMAKE_SOURCE_DIR}/src/unittest_bufferlist.sh)
add_dependencies(check unittest_bufferlist_shell)

add_test(NAME osd_reactivate COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/osd/osd-reactivate.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check osd_reactivate)

add_test(NAME run_tox COMMAND bash ${CMAKE_SOURCE_DIR}/src/ceph-detect-init/run-tox.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check run_tox)

add_test(NAME cephtool_test_osd COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/cephtool-test-osd.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check cephtool_test_osd)

add_test(NAME check_generated COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/encoding/check-generated.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check check_generated)

add_test(NAME run_rbd_unit_tests COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/run-rbd-unit-tests.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check run_rbd_unit_tests)

add_test(NAME osd_pool_create COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/osd-pool-create.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check osd_pool_create)

add_test(NAME mon_handle_forward COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/mon-handle-forward.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check mon_handle_forward)

add_test(NAME osd_erasure_code_profile COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/osd-erasure-code-profile.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check osd_erasure_code_profile)

add_test(NAME cephtool_test_mds COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/cephtool-test-mds.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check cephtool_test_mds)

add_test(NAME readable COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/encoding/readable.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check readable)

add_test(NAME encode_decode_non_regression COMMAND bash ${CMAKE_SOURCE_DIR}/qa/workunits/erasure-code/encode-decode-non-regression.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check encode_decode_non_regression)

# unittest_encoding
add_executable(unittest_encoding EXCLUDE_FROM_ALL
  encoding.cc
  )
add_test(unittest_encoding unittest_encoding)
add_dependencies(check unittest_encoding)
target_link_libraries(unittest_encoding cephfs librados pthread rt m ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_encoding
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_addrs
add_executable(unittest_addrs EXCLUDE_FROM_ALL
  test_addrs.cc
  )
add_test(unittest_addrs unittest_addrs)
add_dependencies(check unittest_addrs)
target_link_libraries(unittest_addrs cephfs librados pthread rt m
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_addrs
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_bloom_filter
add_executable(unittest_bloom_filter EXCLUDE_FROM_ALL
  common/test_bloom_filter.cc
  )
add_test(unittest_bloom_filter unittest_bloom_filter)
add_dependencies(check unittest_bloom_filter)
target_link_libraries(unittest_bloom_filter global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_bloom_filter
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_histogram
add_executable(unittest_histogram EXCLUDE_FROM_ALL
  common/histogram.cc
  )
add_test(unittest_histogram unittest_histogram)
add_dependencies(check unittest_histogram)
target_link_libraries(unittest_histogram global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_histogram
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_prioritized_queue
add_executable(unittest_prioritized_queue EXCLUDE_FROM_ALL
  common/test_prioritized_queue.cc
  )
add_test(unittest_prioritized_queue unittest_prioritized_queue)
add_dependencies(check unittest_prioritized_queue)
target_link_libraries(unittest_prioritized_queue global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_prioritized_queue
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_weighted_priority_queue
add_executable(unittest_weighted_priority_queue EXCLUDE_FROM_ALL
  common/test_weighted_priority_queue.cc
  )
add_test(unittest_weighted_priority_queue unittest_weighted_priority_queue)
add_dependencies(check unittest_weighted_priority_queue)
target_link_libraries(unittest_weighted_priority_queue global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_weighted_priority_queue
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_str_map
add_executable(unittest_str_map EXCLUDE_FROM_ALL
  common/test_str_map.cc
  )
add_test(unittest_str_map unittest_str_map)
add_dependencies(check unittest_str_map)
target_link_libraries(unittest_str_map common global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS} common)
set_target_properties(unittest_str_map
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_shared_mutex
set(unittest_mutex_debug_srcs
  common/test_mutex_debug.cc
  )
add_executable(unittest_mutex_debug EXCLUDE_FROM_ALL
  ${unittest_mutex_debug_srcs}
  )
add_test(unittest_mutex_debug unittest_mutex_debug)
add_dependencies(check unittest_mutex_debug)
target_link_libraries(unittest_mutex_debug global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS} ${EXTRALIBS})
set_target_properties(unittest_mutex_debug
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_shared_mutex
set(unittest_shunique_lock_srcs
  common/test_shunique_lock.cc
  )
add_executable(unittest_shunique_lock EXCLUDE_FROM_ALL
  ${unittest_shunique_lock_srcs}
  )
add_test(unittest_shunique_lock unittest_shunique_lock)
add_dependencies(check unittest_shunique_lock)
target_link_libraries(unittest_shunique_lock global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS} ${EXTRALIBS})
set_target_properties(unittest_shunique_lock
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_sharedptr_registry
add_executable(unittest_sharedptr_registry EXCLUDE_FROM_ALL
  common/test_sharedptr_registry.cc
  )
add_test(unittest_sharedptr_registry unittest_sharedptr_registry)
add_dependencies(check unittest_sharedptr_registry)
target_link_libraries(unittest_sharedptr_registry global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_sharedptr_registry
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_shared_cache
add_executable(unittest_shared_cache EXCLUDE_FROM_ALL
  common/test_shared_cache.cc
  )
add_test(unittest_shared_cache unittest_shared_cache)
add_dependencies(check unittest_shared_cache)
target_link_libraries(unittest_shared_cache global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_shared_cache
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_sloppy_crc_map
add_executable(unittest_sloppy_crc_map EXCLUDE_FROM_ALL
  common/test_sloppy_crc_map.cc
  )
add_test(unittest_sloppy_crc_map unittest_sloppy_crc_map)
add_dependencies(check unittest_sloppy_crc_map)
target_link_libraries(unittest_sloppy_crc_map global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_sloppy_crc_map
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_time
set(unittest_time_srcs
  common/test_time.cc
  ${CMAKE_SOURCE_DIR}/src/common/ceph_time.cc
  )
add_executable(unittest_time
  ${unittest_time_srcs}
  )
target_link_libraries(unittest_time
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_time
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_util
add_executable(unittest_util EXCLUDE_FROM_ALL
  common/test_util.cc
  ${CMAKE_SOURCE_DIR}/src/common/util.cc
  )
add_test(unittest_util unittest_util)
add_dependencies(check unittest_util)
target_link_libraries(unittest_util
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_util
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_crush_wrapper
add_executable(unittest_crush_wrapper EXCLUDE_FROM_ALL
  crush/CrushWrapper.cc
  )
add_test(unittest_crush_wrapper unittest_crush_wrapper)
add_dependencies(check unittest_crush_wrapper)
target_link_libraries(unittest_crush_wrapper global crush ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_crush_wrapper PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_crush
add_executable(unittest_crush EXCLUDE_FROM_ALL
  crush/crush.cc
  )
add_test(unittest_crush unittest_crush)
add_dependencies(check unittest_crush)
target_link_libraries(unittest_crush global m ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS} ${EXTRALIBS})
set_target_properties(unittest_crush PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_osdmap
add_executable(unittest_osdmap EXCLUDE_FROM_ALL
  osd/TestOSDMap.cc
  )
add_test(unittest_osdmap unittest_osdmap)
add_dependencies(check unittest_osdmap)
target_link_libraries(unittest_osdmap global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_osdmap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_workqueue
add_executable(unittest_workqueue EXCLUDE_FROM_ALL
  test_workqueue.cc
  )
add_test(unittest_workqueue unittest_workqueue)
add_dependencies(check unittest_workqueue)
target_link_libraries(unittest_workqueue global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_workqueue PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})


# unittest_striper
add_executable(unittest_striper EXCLUDE_FROM_ALL
  test_striper.cc
  )
add_test(unittest_striper unittest_striper)
add_dependencies(check unittest_striper)
target_link_libraries(unittest_striper global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_striper PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_prebufferedstreambuf
add_executable(unittest_prebufferedstreambuf EXCLUDE_FROM_ALL
  test_prebufferedstreambuf.cc
  )
add_test(unittest_prebufferedstreambuf unittest_prebufferedstreambuf)
add_dependencies(check unittest_prebufferedstreambuf)
target_link_libraries(unittest_prebufferedstreambuf global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_prebufferedstreambuf PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_str_list
add_executable(unittest_str_list EXCLUDE_FROM_ALL
  test_str_list.cc
  )
add_test(unittest_str_list unittest_str_list)
add_dependencies(check unittest_str_list)
target_link_libraries(unittest_str_list global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_str_list PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_log
add_executable(unittest_log EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/log/test.cc
  )
add_test(unittest_log unittest_log)
add_dependencies(check unittest_log)
target_link_libraries(unittest_log global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_throttle
add_executable(unittest_throttle EXCLUDE_FROM_ALL
  common/Throttle.cc
  )
add_test(unittest_throttle unittest_throttle)
add_dependencies(check unittest_throttle)
target_link_libraries(unittest_throttle global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_throttle PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_base64
add_executable(unittest_base64 EXCLUDE_FROM_ALL
  base64.cc
  )
add_test(unittest_base64 unittest_base64)
add_dependencies(check unittest_base64)
target_link_libraries(unittest_base64 global ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_base64 PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_ceph_argparse
add_executable(unittest_ceph_argparse EXCLUDE_FROM_ALL
  ceph_argparse.cc
  )
add_test(unittest_ceph_argparse unittest_ceph_argparse)
add_dependencies(check unittest_ceph_argparse)
target_link_libraries(unittest_ceph_argparse global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_argparse PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ceph_compatset
add_executable(unittest_ceph_compatset EXCLUDE_FROM_ALL
  ceph_compatset.cc
  )
add_test(unittest_ceph_compatset unittest_ceph_compatset)
add_dependencies(check unittest_ceph_compatset)
target_link_libraries(unittest_ceph_compatset global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_compatset PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mds_types
add_executable(unittest_mds_types EXCLUDE_FROM_ALL
  fs/mds_types.cc
  )
add_test(unittest_mds_types unittest_mds_types)
add_dependencies(check unittest_mds_types)
target_link_libraries(unittest_mds_types global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_mds_types PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_osd_types
add_executable(unittest_osd_types EXCLUDE_FROM_ALL
  osd/types.cc
  )
add_test(unittest_osd_types unittest_osd_types)
add_dependencies(check unittest_osd_types)
target_link_libraries(unittest_osd_types global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_osd_types PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_lru
add_executable(unittest_lru EXCLUDE_FROM_ALL
  common/test_lru.cc
  )
add_test(unittest_lru unittest_lru)
add_dependencies(check unittest_lru)
target_link_libraries(unittest_lru global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_lru PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_io_priority
add_executable(unittest_io_priority EXCLUDE_FROM_ALL
  common/test_io_priority.cc
  )
add_test(unittest_io_priority unittest_io_priority)
add_dependencies(check unittest_io_priority)
target_link_libraries(unittest_io_priority global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_io_priority PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_gather
add_executable(unittest_gather EXCLUDE_FROM_ALL
  gather.cc
  )
add_test(unittest_gather unittest_gather)
add_dependencies(check unittest_gather)
target_link_libraries(unittest_gather global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_gather PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# run_cmd
add_executable(unittest_run_cmd EXCLUDE_FROM_ALL
  run_cmd.cc
  )
add_test(unittest_run_cmd unittest_run_cmd)
add_dependencies(check unittest_run_cmd)
target_link_libraries(unittest_run_cmd global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_run_cmd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# signals
add_executable(unittest_signals EXCLUDE_FROM_ALL
  signals.cc
  )
add_test(unittest_signals unittest_signals)
add_dependencies(check unittest_signals)
target_link_libraries(unittest_signals global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_signals PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_simple_spin
add_executable(unittest_simple_spin EXCLUDE_FROM_ALL
  simple_spin.cc
  )
add_test(unittest_simple_spin unittest_simple_spin)
add_dependencies(check unittest_simple_spin)
target_link_libraries(unittest_simple_spin global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_simple_spin PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_bufferlist
add_executable(unittest_bufferlist EXCLUDE_FROM_ALL
  bufferlist.cc
  )
add_test(unittest_bufferlist unittest_bufferlist)
add_dependencies(check unittest_bufferlist)
target_link_libraries(unittest_bufferlist global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_bufferlist PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_xlist
add_executable(unittest_xlist EXCLUDE_FROM_ALL
  test_xlist.cc
  )
add_test(unittest_xlist unittest_xlist)
add_dependencies(check unittest_xlist)
target_link_libraries(unittest_xlist common ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_xlist PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_librados
add_executable(unittest_librados EXCLUDE_FROM_ALL
  librados/librados.cc
  )
add_test(unittest_librados unittest_librados)
add_dependencies(check unittest_librados)
target_link_libraries(unittest_librados
  librados
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_librados PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_crc32c
add_executable(unittest_crc32c EXCLUDE_FROM_ALL
  common/test_crc32c.cc
  )
add_test(unittest_crc32c unittest_crc32c)
add_dependencies(check unittest_crc32c)
target_link_libraries(unittest_crc32c global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_crc32c PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_arch
add_executable(unittest_arch EXCLUDE_FROM_ALL
  test_arch.cc
  )
add_test(unittest_arch unittest_arch)
add_dependencies(check unittest_arch)
target_link_libraries(unittest_arch global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_arch PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_crypto
add_executable(unittest_crypto
  crypto.cc
  )
add_test(unittest_crypto unittest_crypto)
add_dependencies(check unittest_crypto)
target_link_libraries(unittest_crypto global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_crypto PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_crypto_init
add_executable(unittest_crypto_init EXCLUDE_FROM_ALL
  crypto_init.cc
  )
add_test(unittest_crypto_init unittest_crypto_init)
add_dependencies(check unittest_crypto_init)
target_link_libraries(unittest_crypto_init global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_crypto_init PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_perf_counters
add_executable(unittest_perf_counters EXCLUDE_FROM_ALL
  perf_counters.cc
  )
add_test(unittest_perf_counters unittest_perf_counters)
add_dependencies(check unittest_perf_counters)
target_link_libraries(unittest_perf_counters global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_perf_counters PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ceph_crypto
add_executable(unittest_ceph_crypto EXCLUDE_FROM_ALL
  ceph_crypto.cc
  )
add_test(unittest_ceph_crypto unittest_ceph_crypto)
add_dependencies(check unittest_ceph_crypto)
target_link_libraries(unittest_ceph_crypto global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_crypto PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_utf8
add_executable(unittest_utf8 EXCLUDE_FROM_ALL
  utf8.cc
  )
add_test(unittest_utf8 unittest_utf8)
add_dependencies(check unittest_utf8)
target_link_libraries(unittest_utf8 global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_utf8 PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mime
add_executable(unittest_mime EXCLUDE_FROM_ALL
  mime.cc
  )
add_test(unittest_mime unittest_mime)
add_dependencies(check unittest_mime)
target_link_libraries(unittest_mime global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_mime PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_escape
add_executable(unittest_escape EXCLUDE_FROM_ALL
  escape.cc
  )
add_test(unittest_escape unittest_escape)
add_dependencies(check unittest_escape)
target_link_libraries(unittest_escape global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_escape PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_strtol
add_executable(unittest_strtol EXCLUDE_FROM_ALL
  strtol.cc
  )
add_test(unittest_strtol unittest_strtol)
add_dependencies(check unittest_strtol)
target_link_libraries(unittest_strtol global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_strtol PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_confutils
add_executable(unittest_confutils EXCLUDE_FROM_ALL
  confutils.cc
  )
add_test(unittest_confutils unittest_confutils)
add_dependencies(check unittest_confutils)
target_link_libraries(unittest_confutils global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_confutils PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_config
add_executable(unittest_config EXCLUDE_FROM_ALL
  common/test_config.cc
  )
add_test(unittest_config unittest_config)
add_dependencies(check unittest_config)
target_link_libraries(unittest_config global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_context
add_executable(unittest_context EXCLUDE_FROM_ALL
  common/test_context.cc
  )
add_test(unittest_context unittest_context)
add_dependencies(check unittest_context)
target_link_libraries(unittest_context global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_context PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_safe_io
add_executable(unittest_safe_io EXCLUDE_FROM_ALL
  common/test_safe_io.cc
  )
add_test(unittest_safe_io unittest_safe_io)
add_dependencies(check unittest_safe_io)
target_link_libraries(unittest_safe_io global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_safe_io PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_heartbeatmap
add_executable(unittest_heartbeatmap EXCLUDE_FROM_ALL
  heartbeat_map.cc
  )
add_test(unittest_heartbeatmap unittest_heartbeatmap)
add_dependencies(check unittest_heartbeatmap)
target_link_libraries(unittest_heartbeatmap global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_heartbeatmap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

if(${WITH_RADOSGW})
  # unittest_formatter
  # why does this include rgw/rgw_formats.cc...?
  add_executable(unittest_formatter EXCLUDE_FROM_ALL
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_formats.cc
  )
  add_test(unittest_formatter unittest_formatter)
  add_dependencies(check unittest_formatter)
  target_link_libraries(unittest_formatter global ${CMAKE_DL_LIBS}
    ${UNITTEST_LIBS})
  set_target_properties(unittest_formatter PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
endif(${WITH_RADOSGW})

# unittest_daemon_config
add_executable(unittest_daemon_config EXCLUDE_FROM_ALL
  daemon_config.cc
  )
add_test(unittest_daemon_config unittest_daemon_config)
add_dependencies(check unittest_daemon_config)
target_link_libraries(unittest_daemon_config
  common
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  )
set_target_properties(unittest_daemon_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_libcephfs_config
add_executable(unittest_libcephfs_config EXCLUDE_FROM_ALL
  libcephfs_config.cc
  )
add_test(unittest_libcephfs_config unittest_libcephfs_config)
add_dependencies(check unittest_libcephfs_config)
target_link_libraries(unittest_libcephfs_config cephfs ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_libcephfs_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_lfnindex
add_executable(unittest_lfnindex EXCLUDE_FROM_ALL
  os/TestLFNIndex.cc
  )
add_test(unittest_lfnindex unittest_lfnindex)
add_dependencies(check unittest_lfnindex)
target_link_libraries(unittest_lfnindex os global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_lfnindex PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_librados_config
set(unittest_librados_config_srcs librados/librados_config.cc)
add_executable(unittest_librados_config EXCLUDE_FROM_ALL
  ${unittest_librados_config_srcs}
  )
add_test(unittest_librados_config unittest_librados_config)
add_dependencies(check unittest_librados_config)
target_link_libraries(unittest_librados_config
  librados
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_librados_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_rbd_replay
add_executable(unittest_rbd_replay EXCLUDE_FROM_ALL
  test_rbd_replay.cc
  $<TARGET_OBJECTS:parse_secret_objs>
)
add_test(unittest_rbd_replay unittest_rbd_replay)
add_dependencies(check unittest_rbd_replay)
target_link_libraries(unittest_rbd_replay
  librbd
  librados
  global
  rbd_replay
  rbd_replay_ios
  keyutils
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS}
)
set_target_properties(unittest_rbd_replay PROPERTIES COMPILE_FLAGS
${UNITTEST_CXX_FLAGS})

# unittest_mon_moncap
add_executable(unittest_mon_moncap EXCLUDE_FROM_ALL
  ${unittest_mon_moncap_srcs}
  mon/moncap.cc
  )
add_test(unittest_mon_moncap unittest_mon_moncap)
add_dependencies(check unittest_mon_moncap)
target_link_libraries(unittest_mon_moncap mon global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_mon_moncap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mon_pgmap
add_executable(unittest_mon_pgmap EXCLUDE_FROM_ALL
  mon/PGMap.cc
  ${unittest_mon_moncap_srcs}
  )
add_test(unittest_mon_pgmap unittest_mon_pgmap)
add_dependencies(check unittest_mon_pgmap)
target_link_libraries(unittest_mon_pgmap mon global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_mon_pgmap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ecbackend
add_executable(unittest_ecbackend EXCLUDE_FROM_ALL
  osd/TestECBackend.cc
  )
add_test(unittest_ecbackend unittest_ecbackend)
add_dependencies(check unittest_ecbackend)
target_link_libraries(unittest_ecbackend osd global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_ecbackend PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_osdscrub
add_executable(unittest_osdscrub EXCLUDE_FROM_ALL
  osd/TestOSDScrub.cc
  )
add_test(unittest_osdscrub unittest_osdscrub)
add_dependencies(check unittest_osdscrub)
target_link_libraries(unittest_osdscrub osd global dl os mon ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${UNITTEST_LIBS})
set_target_properties(unittest_osdscrub PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_pglog
add_executable(unittest_pglog EXCLUDE_FROM_ALL
  osd/TestPGLog.cc
  )
add_test(unittest_pglog unittest_pglog)
add_dependencies(check unittest_pglog)
target_link_libraries(unittest_pglog osd global dl ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${UNITTEST_LIBS})
set_target_properties(unittest_pglog PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_hitset
add_executable(unittest_hitset EXCLUDE_FROM_ALL
  osd/hitset.cc
  )
add_test(unittest_hitset unittest_hitset)
add_dependencies(check unittest_hitset)
target_link_libraries(unittest_hitset osd global ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${UNITTEST_LIBS})
set_target_properties(unittest_hitset PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_osd_osdcap
add_executable(unittest_osd_osdcap EXCLUDE_FROM_ALL
  osd/osdcap.cc
)
add_test(unittest_osd_osdcap unittest_osd_osdcap)
add_dependencies(check unittest_osd_osdcap)
target_link_libraries(unittest_osd_osdcap osd global ${CMAKE_DL_LIBS}
${BLKID_LIBRARIES} ${UNITTEST_LIBS})
set_target_properties(unittest_osd_osdcap PROPERTIES COMPILE_FLAGS
${UNITTEST_CXX_FLAGS})

# unittest_snap_mapper
add_executable(unittest_snap_mapper EXCLUDE_FROM_ALL
  test_snap_mapper.cc
  )
add_test(unittest_snap_mapper unittest_snap_mapper)
add_dependencies(check unittest_snap_mapper)
target_link_libraries(unittest_snap_mapper osd global ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${UNITTEST_LIBS})
set_target_properties(unittest_snap_mapper PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mds_authcap
add_executable(unittest_mds_authcap EXCLUDE_FROM_ALL
  mds/TestMDSAuthCaps.cc
  )
add_test(unittest_mds_authcap unittest_mds_authcap)
add_dependencies(check unittest_mds_authcap)
target_link_libraries(unittest_mds_authcap mds global ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${UNITTEST_LIBS})
set_target_properties(unittest_mds_authcap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mds_sessionfilter
add_executable(unittest_mds_sessionfilter EXCLUDE_FROM_ALL
    mds/TestSessionFilter.cc
  $<TARGET_OBJECTS:common_util_obj>
  )
add_test(unittest_mds_sessionfilter unittest_mds_sessionfilter)
add_dependencies(check unittest_mds_sessionfilter)
target_link_libraries(unittest_mds_sessionfilter mds osdc common global
  ${CMAKE_DL_LIBS} ${BLKID_LIBRARIES} ${UNITTEST_LIBS})
set_target_properties(unittest_mds_sessionfilter PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ipaddr
add_executable(unittest_ipaddr EXCLUDE_FROM_ALL
  test_ipaddr.cc)  
add_test(unittest_ipaddr unittest_ipaddr)
add_dependencies(check unittest_ipaddr)
target_link_libraries(unittest_ipaddr mon global ${CMAKE_DL_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_ipaddr PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_texttable
add_executable(unittest_texttable EXCLUDE_FROM_ALL
  test_texttable.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  )
add_test(unittest_texttable unittest_texttable)
add_dependencies(check unittest_texttable)
target_link_libraries(unittest_texttable mon global ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_texttable PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_on_exit
add_executable(unittest_on_exit EXCLUDE_FROM_ALL
  on_exit.cc
  )
add_test(unittest_on_exit unittest_on_exit)
add_dependencies(check unittest_on_exit)
target_link_libraries(unittest_on_exit
  global
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
add_test(unittest_on_exit unittest_on_exit)
add_dependencies(check unittest_on_exit)
set_target_properties(unittest_on_exit PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_readahead
add_executable(unittest_readahead EXCLUDE_FROM_ALL
  common/Readahead.cc
  )
add_test(unittest_readahead unittest_readahead)
add_dependencies(check unittest_readahead)
target_link_libraries(unittest_readahead
  global
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
add_test(unittest_readahead unittest_readahead)
add_dependencies(check unittest_readahead)
set_target_properties(unittest_readahead PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_tableformatter
add_executable(unittest_tableformatter EXCLUDE_FROM_ALL
  common/test_tableformatter.cc
  )
add_test(unittest_tableformatter unittest_tableformatter)
add_dependencies(check unittest_tableformatter)
target_link_libraries(unittest_tableformatter
  global
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
add_test(unittest_tableformatter unittest_tableformatter)
add_dependencies(check unittest_tableformatter)
set_target_properties(unittest_tableformatter PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_bit_vector
add_executable(unittest_bit_vector EXCLUDE_FROM_ALL
  common/test_bit_vector.cc
  )
add_test(unittest_bit_vector unittest_bit_vector)
add_dependencies(check unittest_bit_vector)
target_link_libraries(unittest_bit_vector
  global
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
add_test(unittest_bit_vector unittest_bit_vector)
add_dependencies(check unittest_bit_vector)
set_target_properties(unittest_bit_vector PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_interval_set
add_executable(unittest_interval_set EXCLUDE_FROM_ALL
  common/test_interval_set.cc
  )
add_test(unittest_interval_set unittest_interval_set)
add_dependencies(check unittest_interval_set)
target_link_libraries(unittest_interval_set
  global
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
add_test(unittest_interval_set unittest_interval_set)
add_dependencies(check unittest_interval_set)
set_target_properties(unittest_interval_set PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_subprocess
add_executable(unittest_subprocess EXCLUDE_FROM_ALL
  test_subprocess.cc
  )
add_dependencies(check unittest_subprocess)
target_link_libraries(unittest_subprocess
  global
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_subprocess PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_pageset
add_executable(unittest_pageset EXCLUDE_FROM_ALL test_pageset.cc)
add_test(unittest_pageset unittest_pageset)
add_dependencies(check unittest_pageset)
target_link_libraries(unittest_pageset ${UNITTEST_LIBS})
set_target_properties(unittest_pageset PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_async_compressor
add_executable(unittest_async_compressor EXCLUDE_FROM_ALL
  common/test_async_compressor.cc
)
add_dependencies(check unittest_async_compressor)
target_link_libraries(unittest_async_compressor
  global
  compressor
  ${CMAKE_DL_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_async_compressor PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_rocksdb_option
add_executable(unittest_rocksdb_option EXCLUDE_FROM_ALL
  objectstore/TestRocksdbOptionParse.cc
  )
add_test(unittest_rocksdb_option unittest_rocksdb_option)
add_dependencies(check unittest_rocksdb_option)
target_link_libraries(unittest_rocksdb_option global os ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${UNITTEST_LIBS})
set_target_properties(unittest_rocksdb_option PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_bluefs
add_executable(unittest_bluefs EXCLUDE_FROM_ALL objectstore/test_bluefs.cc)
add_test(unittest_bluefs unittest_bluefs)
add_dependencies(check unittest_bluefs)
target_link_libraries(unittest_bluefs os global ${UNITTEST_LIBS})
set_target_properties(unittest_bluefs PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_bluestore_types
add_executable(unittest_bluestore_types EXCLUDE_FROM_ALL objectstore/test_bluestore_types.cc)
add_test(unittest_bluestore_types unittest_bluestore_types)
add_dependencies(check unittest_bluestore_types)
target_link_libraries(unittest_bluestore_types os global ${UNITTEST_LIBS})
set_target_properties(unittest_bluestore_types PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
  
add_subdirectory(erasure-code EXCLUDE_FROM_ALL)

#make check ends here

if(${WITH_RADOSGW})
  add_subdirectory(rgw)
  # test_cors
  set(test_cors_srcs test_cors.cc)
  add_executable(test_cors
    ${test_cors_srcs}
    )
  target_link_libraries(test_cors
    librados
    rgw_a
    global
    curl
    expat
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})
  set_target_properties(test_cors PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_rgw_manifest
  set(test_rgw_manifest_srcs rgw/test_rgw_manifest.cc)
  add_executable(test_rgw_manifest
    ${test_rgw_manifest_srcs}
    )
  target_link_libraries(test_rgw_manifest
    rgw_a
    ${CMAKE_DL_LIBS}
    ${UNITTEST_LIBS}
    ${CRYPTO_LIBS}
    )
  set_target_properties(test_rgw_manifest PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_rgw_obj
  set(test_rgw_obj_srcs rgw/test_rgw_obj.cc)
  add_executable(test_rgw_obj
    ${test_rgw_obj_srcs}
    )
  target_link_libraries(test_rgw_obj
    rgw_a
    cls_rgw_client
    cls_lock_client
    cls_refcount_client
    cls_log_client
    cls_statelog_client
    cls_version_client
    cls_replica_log_client
    cls_kvs
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
  set_target_properties(test_rgw_obj PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_meta
  set(test_cls_rgw_meta_srcs test_rgw_admin_meta.cc)
  add_executable(test_cls_rgw_meta
    ${test_cls_rgw_meta_srcs}
    )
  target_link_libraries(test_cls_rgw_meta
    rgw_a
    boost_regex
    ${Boost_REGEX_LIBRARY}
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS} ${UNITTEST_LIBS} ${CRYPTO_LIBS})
  set_target_properties(test_cls_rgw_meta PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_log
  set(test_cls_rgw_log_srcs
    test_rgw_admin_log.cc
    )
  add_executable(test_cls_rgw_log
    ${test_cls_rgw_log_srcs}
    )
  target_link_libraries(test_cls_rgw_log
    rgw_a
    global
    curl
    expat
    cls_version_client
    cls_log_client
    cls_statelog_client
    cls_refcount_client
    cls_rgw_client
    cls_user_client
    cls_lock_client
    cls_timeindex_client
    ${Boost_REGEX_LIBRARY}
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CRYPTO_LIBS}
    )
  set_target_properties(test_cls_rgw_log PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_opstate
  set(test_cls_rgw_opstate_srcs test_rgw_admin_opstate.cc)
  add_executable(test_cls_rgw_opstate
    ${test_cls_rgw_opstate_srcs}
    )
  target_link_libraries(test_cls_rgw_opstate
    rgw_a
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    ${UNITTEST_LIBS}
    ${CRYPTO_LIBS}
    ${EXTRALIBS}
    )
  set_target_properties(test_cls_rgw_opstate PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
endif(${WITH_RADOSGW})

# radostest 
set(libradostest_srcs librados/test.cc librados/TestCase.cc)
add_library(radostest STATIC ${libradostest_srcs})
set_target_properties(radostest PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# multi_stress_watch
add_executable(multi_stress_watch
  multi_stress_watch.cc
  )
target_link_libraries(multi_stress_watch librados global radostest
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}) 

set(librados_test_stub_srcs
  librados_test_stub/LibradosTestStub.cc
  librados_test_stub/TestClassHandler.cc
  librados_test_stub/TestIoCtxImpl.cc
  librados_test_stub/TestMemIoCtxImpl.cc
  librados_test_stub/TestMemRadosClient.cc
  librados_test_stub/TestRadosClient.cc)
add_library(librados_test_stub STATIC ${librados_test_stub_srcs})
  
# unittest_librbd
add_executable(unittest_librbd EXCLUDE_FROM_ALL
  librbd/test_librbd.cc
  librbd/test_fixture.cc
  librbd/test_ImageWatcher.cc
  librbd/test_internal.cc
  librbd/test_mirroring.cc
  librbd/test_support.cc
  librbd/test_main.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  ${CMAKE_SOURCE_DIR}/src/common/secret.c
  )
add_dependencies(check unittest_librbd)
set_target_properties(unittest_librbd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
if(${WITH_LTTNG})
  target_link_libraries(unittest_librbd librbd_tp)
endif(${WITH_LTTNG})
target_link_libraries(unittest_librbd
  librbd
  librados
  journal
  cls_journal_client
  librados_api
  librados_test_stub
  ${UNITTEST_LIBS}
  global
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  blkid
  udev
  keyutils
  radostest
  )

add_executable(test_librbd_fsx
  librbd/fsx.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  ${CMAKE_SOURCE_DIR}/src/common/secret.c
  )
target_link_libraries(test_librbd_fsx
  librbd
  librados
  global
  m
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  blkid
  udev
  keyutils
  )

add_executable(test_cls_rbd
  cls_rbd/test_cls_rbd.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  ${CMAKE_SOURCE_DIR}/src/common/secret.c
  )
set_target_properties(test_cls_rbd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_rbd
  librbd
  cls_rbd
  cls_lock
  librados
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  blkid
  udev
  keyutils
  )

add_executable(test_cls_refcount
  cls_refcount/test_cls_refcount.cc
  )
set_target_properties(test_cls_refcount PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_refcount
  librados
  cls_refcount_client
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_version
  cls_version/test_cls_version.cc
  )
set_target_properties(test_cls_version PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_version
  librados
  cls_version_client
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_log
  cls_log/test_cls_log.cc
  )
set_target_properties(test_cls_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_log
  librados
  cls_log_client
  global
  radostest
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )

add_executable(test_cls_statelog
  cls_statelog/test_cls_statelog.cc
  )
set_target_properties(test_cls_statelog PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_statelog
  librados
  cls_statelog_client
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_replica_log
  cls_replica_log/test_cls_replica_log.cc
  )
set_target_properties(test_cls_replica_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_replica_log
  librados
  cls_replica_log_client
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_lock
  cls_lock/test_cls_lock.cc
  )
set_target_properties(test_cls_lock PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_lock
  cls_lock
  librados
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_hello
  cls_hello/test_cls_hello.cc
  )
set_target_properties(test_cls_hello PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_hello
  librados
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest
  ${UNITTEST_LIBS}
  )

add_executable(test_cls_numops cls_numops/test_cls_numops.cc)
set_target_properties(test_cls_numops PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_numops 
  librados 
  global
  cls_numops_client 
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest
  ${UNITTEST_LIBS} 
  )

if(${WITH_RADOSGW})
  add_executable(test_cls_rgw
    cls_rgw/test_cls_rgw.cc
    )
  set_target_properties(test_cls_rgw PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(test_cls_rgw
    cls_rgw_client
    librados
    global
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    radostest)
endif(${WITH_RADOSGW})

# test_async_driver
add_executable(test_async_driver
  msgr/test_async_driver.cc
  )
set_target_properties(test_async_driver PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_async_driver os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

# test_msgr
add_executable(test_msgr
  msgr/test_msgr.cc
  )
set_target_properties(test_msgr PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_msgr os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

#test_mon_msg
add_executable(test_mon_msg mon/test-mon-msg.cc)
set_target_properties(test_mon_msg PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_mon_msg os osdc global ${UNITTEST_LIBS})

#test_perf_local
add_executable(test_perf_local 
  perf_local.cc
  perf_helper.cc)
#INTEL_SSE & INTEL_SSE2 flags
set(PERF_LOCAL_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse2") 
set_target_properties(test_perf_local PROPERTIES COMPILE_FLAGS
  ${PERF_LOCAL_FLAGS})
target_link_libraries(test_perf_local os global ${UNITTEST_LIBS})

#test_perf_objectstore
add_executable(test_perf_objectstore objectstore/ObjectStoreTransactionBenchmark.cc)
set_target_properties(test_perf_objectstore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_perf_objectstore os osdc global ${UNITTEST_LIBS})

#test_perf_msgr_server
add_executable(test_perf_msgr_server msgr/perf_msgr_server.cc)
set_target_properties(test_perf_msgr_server PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_perf_msgr_server os global ${UNITTEST_LIBS})

#test_perf_msgr_client
add_executable(test_perf_msgr_client msgr/perf_msgr_client.cc)
set_target_properties(test_perf_msgr_client PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_perf_msgr_client os global ${UNITTEST_LIBS})

add_executable(test_mon_workloadgen
  mon/test_mon_workloadgen.cc
  )
target_link_libraries(test_mon_workloadgen
  os
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_rados_api_cmd
  librados/cmd.cc
  )
set_target_properties(test_rados_api_cmd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_cmd
  librados global ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_io
  librados/io.cc
  )
set_target_properties(test_rados_api_io PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_io
  librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_c_write_operations
  librados/c_write_operations.cc
  )
set_target_properties(test_rados_api_c_write_operations PROPERTIES
  COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_c_write_operations
  librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_c_read_operations
  librados/c_read_operations.cc
  )
set_target_properties(test_rados_api_c_read_operations PROPERTIES COMPILE_FLAGS 
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_c_read_operations
  librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_aio
  librados/aio.cc
  )
set_target_properties(test_rados_api_aio PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_aio
  librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_list
  librados/list.cc
  )
set_target_properties(test_rados_api_list PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_list
  librados global ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_nlist
  librados/nlist.cc
  )
set_target_properties(test_rados_api_nlist PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_nlist
  librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_pool
  librados/pool.cc
  )
set_target_properties(test_rados_api_pool PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS}
  )
target_link_libraries(test_rados_api_pool
  librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_stat
  librados/stat.cc
  )
set_target_properties(test_rados_api_stat PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_stat
  librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_watch_notify
  librados/watch_notify.cc
  )
set_target_properties(test_rados_api_watch_notify PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_watch_notify
  librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_cls
  librados/cls.cc
  )
set_target_properties(test_rados_api_cls PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_cls
  librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_misc
  librados/misc.cc
  )
set_target_properties(test_rados_api_misc PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_misc
  librados global ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_tmap_migrate
  ../tools/cephfs/DataScan.cc
  ../tools/cephfs/MDSUtility.cc
  librados/tmap_migrate.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_tmap_migrate PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_tmap_migrate
  librados mds osdc global cls_cephfs_client ${UNITTEST_LIBS} ${ALLOC_LIBS} radostest)

add_executable(test_rados_api_lock
  librados/lock.cc
  )
set_target_properties(test_rados_api_lock PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_lock
  librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_tier
  librados/tier.cc
  )
set_target_properties(test_rados_api_tier PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_tier
  global librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_snapshots
  librados/snapshots.cc
  )
set_target_properties(test_rados_api_snapshots PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_snapshots
  librados ${UNITTEST_LIBS} radostest)

add_library(rados_striper_test STATIC libradosstriper/TestCase.cc)
target_link_libraries(rados_striper_test radostest)
set_target_properties(rados_striper_test PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_executable(test_rados_api_striping
  libradosstriper/striping.cc
  )
target_link_libraries(test_rados_api_striping librados libradosstriper 
  ${UNITTEST_LIBS} rados_striper_test)
set_target_properties(test_rados_api_striping PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

if(${WITH_CEPHFS})
  add_executable(test_libcephfs
    libcephfs/test.cc
    libcephfs/readdir_r_cb.cc
    libcephfs/caps.cc
    libcephfs/multiclient.cc
    libcephfs/flock.cc
    libcephfs/acl.cc
    libcephfs/access.cc
  )
  set_target_properties(test_libcephfs PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(test_libcephfs
    cephfs
    librados
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
endif(${WITH_CEPHFS})  

add_executable(test_objectstore
  objectstore/store_test.cc
  )
set_target_properties(test_objectstore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_objectstore
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_keyvaluedb
  objectstore/test_kv.cc
  )
set_target_properties(test_keyvaluedb PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_keyvaluedb
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_objectstore_workloadgen
  objectstore/workload_generator.cc
  objectstore/TestObjectStoreState.cc
  )
target_link_libraries(test_objectstore_workloadgen
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

# test_transaction
add_executable(unittest_transaction objectstore/test_transaction.cc)
add_test(unittest_transaction unittest_transaction)
set_target_properties(unittest_transaction PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_transaction os common ${UNITTEST_LIBS})

#test_filestore
add_executable(test_filestore filestore/TestFileStore.cc)
set_target_properties(test_filestore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_filestore os global ${UNITTEST_LIBS})

add_executable(test_filestore_idempotent
  objectstore/test_idempotent.cc
  objectstore/FileStoreTracker.cc
  common/ObjectContents.cc
  )
target_link_libraries(test_filestore_idempotent
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_filestore_idempotent_sequence
  objectstore/test_idempotent_sequence.cc
  objectstore/DeterministicOpSequence.cc
  objectstore/TestObjectStoreState.cc
  objectstore/FileStoreDiff.cc
  )
target_link_libraries(test_filestore_idempotent_sequence
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_xattr_bench
  xattr_bench.cc
  )
set_target_properties(test_xattr_bench PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_xattr_bench
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_filejournal
  test_filejournal.cc
  )
set_target_properties(test_filejournal PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_filejournal
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  )

add_executable(test_stress_watch
  test_stress_watch.cc
  )
set_target_properties(test_stress_watch PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_stress_watch
  librados
  global
  ${UNITTEST_LIBS}
  radostest
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_objectcacher_stress
  osdc/object_cacher_stress.cc
  osdc/FakeWriteback.cc
  )
target_link_libraries(test_objectcacher_stress
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_object_map
  ObjectMap/test_object_map.cc
  ObjectMap/KeyValueDBMemory.cc
  )
set_target_properties(test_object_map PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_object_map
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_keyvaluedb_atomicity
  ObjectMap/test_keyvaluedb_atomicity.cc
  )
set_target_properties(test_keyvaluedb_atomicity PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_keyvaluedb_atomicity
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_keyvaluedb_iterators
  ObjectMap/test_keyvaluedb_iterators.cc
  ObjectMap/KeyValueDBMemory.cc
  )
set_target_properties(test_keyvaluedb_iterators PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_keyvaluedb_iterators
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# librgw_file (nfs-like RGW interface)
add_executable(test_librgw_file
  librgw_file.cc
  )
set_target_properties(test_librgw_file PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_librgw_file
  rgw
  librados
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# librgw_file_cd (just the rgw_file create-delete bucket ops)
add_executable(test_librgw_file_cd
  librgw_file_cd.cc
  )
set_target_properties(test_librgw_file_cd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_librgw_file_cd
  rgw
  librados
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# librgw_file_gp (just the rgw_file get-put bucket ops)
add_executable(test_librgw_file_gp
  librgw_file_gp.cc
  )
set_target_properties(test_librgw_file_gp PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_librgw_file_gp
  rgw
  librados
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# librgw_file_nfsns (nfs namespace tests)
add_executable(test_librgw_file_nfsns
  librgw_file_nfsns.cc
  )
set_target_properties(test_librgw_file_nfsns PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_librgw_file_nfsns
  rgw
  librados
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# librgw_file_aw (nfs write transaction [atomic write] tests)
add_executable(test_librgw_file_aw
  librgw_file_aw.cc
  )
set_target_properties(test_librgw_file_aw PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_librgw_file_aw
  rgw
  librados
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_rgw_token
  test_rgw_token.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rgw_token PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rgw_token
  rgw
  os
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

if(${HAVE_LIBFUSE})
  add_executable(test_cfuse_cache_invalidate
    test_cfuse_cache_invalidate.cc
    )
  target_link_libraries(test_cfuse_cache_invalidate
    global
    os
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
endif(${HAVE_LIBFUSE})

# librgw_file_gp (just the rgw_file get-put bucket ops)
add_executable(test_rgw_ldap
  ../rgw/rgw_ldap.cc
  test_rgw_ldap.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rgw_ldap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rgw_ldap
  librados
  ${OPENLDAP_LIBS}
  ${Boost_LIBRARIES}
  ${ALLOC_LIBS}
  ${UNITTEST_LIBS}
  )

if(${WITH_CEPHFS})
  add_executable(test_c_headers
    test_c_headers.c
  )
  target_link_libraries(test_c_headers
    librados
    cephfs
    ${EXTRALIBS}
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    )
endif(${WITH_CEPHFS})

add_executable(test_get_blkdev_size
  test_get_blkdev_size.cc
  )
target_link_libraries(test_get_blkdev_size
  common
  pthread
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

add_executable(simple_server
  messenger/simple_server.cc
  messenger/simple_dispatcher.cc
  )
target_link_libraries(simple_server
  os global common ${Boost_REGEX_LIBRARY}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(simple_client
  messenger/simple_client.cc
  messenger/simple_dispatcher.cc
  )
target_link_libraries(simple_client
  os global common ${Boost_REGEX_LIBRARY}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

if(HAVE_XIO)
  add_executable(xio_server
    messenger/xio_server.cc
    messenger/xio_dispatcher.cc
    )
  target_link_libraries(xio_server
    os global common ${Boost_REGEX_LIBRARY}
    ${Xio_LIBRARY} ibverbs rdmacm pthread rt
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )

  add_executable(xio_client
    messenger/xio_client.cc
    messenger/xio_dispatcher.cc
    )
  target_link_libraries(xio_client
    os global common ${Boost_REGEX_LIBRARY}
    ${Xio_LIBRARY} ibverbs rdmacm pthread rt
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
endif(HAVE_XIO)

