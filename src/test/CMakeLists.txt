# test_timers
add_executable(test_timers
  TestTimers.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_timers global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_signal_handlers
add_executable(test_signal_handlers
  TestSignalHandlers.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_signal_handlers global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS})

# test_msgr
add_executable(test_msgr
  testmsgr.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_msgr global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_crypt
add_executable(test_crypt
  testcrypto.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_crypt
  global
  ${CRYPTO_LIBS}
  m
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  )

# test_rados
add_executable(test_rados 
  osd/TestRados.cc
  osd/TestOpStat.cc
  osd/Object.cc osd/RadosModel.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_rados
  librados
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  )

# test_mutate
add_executable(test_mutate
  test_mutate.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_mutate global librados ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_rewrite_latency
add_executable(test_rewrite_latency
  test_rewrite_latency.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_rewrite_latency common
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})

# streamtest
add_executable(streamtest
  streamtest.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(streamtest os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_trans
add_executable(test_trans
  test_trans.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_trans os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_keys
add_executable(test_keys
  testkeys.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_keys mon global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# get_command_descriptions
add_executable(get_command_descriptions
  common/get_command_descriptions.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  $<TARGET_OBJECTS:os_mon_objs>
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(get_command_descriptions
  mon
  global
  leveldb
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${TCMALLOC_LIBS}
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
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(smalliobench librados boost_program_options global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# smalliobenchfs
set(smalliobenchfs_srcs
  bench/small_io_bench_fs.cc
  bench/testfilestore_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(smalliobenchfs
  ${smalliobenchfs_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(smalliobenchfs librados boost_program_options os global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# smalliobenchdumb
set(smalliobenchdumb_srcs
  bench/small_io_bench_dumb.cc
  bench/dumb_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(smalliobenchdumb
  ${smalliobenchdumb_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(smalliobenchdumb librados boost_program_options os global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

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
    $<TARGET_OBJECTS:heap_profiler_objs>
    )
  target_link_libraries(smalliobenchrbd
    librbd
    librados
    os
    global
    boost_program_options
    blkid
    udev
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    ${TCMALLOC_LIBS}
    keyutils
    )
endif (${WITH_RBD})

# tpbench
set(tpbench_srcs
  bench/tp_bench.cc
  bench/detailed_stat_collector.cc)
add_executable(tpbench
  ${tpbench_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(tpbench librados boost_program_options os global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# omapbench
set(omapbench_srcs
  omap_bench.cc
  )
add_executable(omapbench
  ${omapbench_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(omapbench
  librados
  boost_program_options
  os
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  )

# kvstorebench
set(kvstorebench_srcs
  kv_store_bench.cc
  ${CMAKE_SOURCE_DIR}/src/key_value_store/kv_flat_btree_async.cc
  )
add_executable(kvstorebench
  ${kvstorebench_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(kvstorebench librados global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS})

# ceph_objectstore_bench
add_executable(ceph_objectstore_bench objectstore_bench.cc
  $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(ceph_objectstore_bench global os ${TCMALLOC_LIBS})

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
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_rados_list_parallel librados systest global pthread
  rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_rados_open_pools_parallel
set(test_rados_open_pools_parallel_srcs system/rados_open_pools_parallel.cc)
add_executable(test_rados_open_pools_parallel
  ${test_rados_open_pools_parallel_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_rados_open_pools_parallel librados systest global
  pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_rados_delete_pools_parallel
set(test_rados_delete_pools_parallel_srcs
  system/rados_delete_pools_parallel.cc
  system/st_rados_create_pool.cc
  system/st_rados_delete_pool.cc
  system/st_rados_list_objects.cc

  )
add_executable(test_rados_delete_pools_parallel
  ${test_rados_delete_pools_parallel_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_rados_delete_pools_parallel librados systest global
  pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

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
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_rados_watch_notify librados systest global
  pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# bench_log
set(bench_log_srcs
  bench_log.cc
  )
add_executable(bench_log
  ${bench_log_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(bench_log global pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

## Unit tests
#make check starts here
add_custom_target(symlinks  COMMAND
  ln -sf ${CMAKE_SOURCE_DIR}/src/test/ ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/ceph-mon ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/ceph ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/ceph-authtool ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/ceph-conf ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/ceph-osd ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_SOURCE_DIR}/src/ceph-disk ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_SOURCE_DIR}/qa/ ${CMAKE_BINARY_DIR}/src/
  COMMENT "Symlinks for test scripts have been created")
add_dependencies(check symlinks)

add_test(NAME unittest_bufferlist_shell COMMAND bash ${CMAKE_SOURCE_DIR}/src/unittest_bufferlist.sh)
add_dependencies(check unittest_bufferlist_shell)

add_test(NAME run_tox COMMAND bash ${CMAKE_SOURCE_DIR}/src/ceph-detect-init/run-tox.sh)
add_dependencies(check run_tox)

add_test(NAME cephtool_test_osd COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/cephtool-test-osd.sh)
add_dependencies(check cephtool_test_osd)

add_test(NAME check_generated COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/encoding/check-generated.sh)
add_dependencies(check check_generated)

add_test(NAME cephtool_test_rados COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/cephtool-test-rados.sh)
add_dependencies(check cephtool_test_rados)

add_test(NAME run_rbd_unit_tests COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/run-rbd-unit-tests.sh)
add_dependencies(check run_rbd_unit_tests)

add_test(NAME mkfs COMMAND bash	${CMAKE_SOURCE_DIR}/src/test/mon/mkfs.sh)
add_dependencies(check mkfs)

add_test(NAME misc COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/misc.sh)
add_dependencies(check misc)

add_test(NAME osd_config COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/osd/osd-config.sh)
add_dependencies(check osd_config)

add_test(NAME osd_pool_create COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/osd-pool-create.sh)
add_dependencies(check osd_pool_create)

add_test(NAME osd_copy_from COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/osd/osd-copy-from.sh)
add_dependencies(check osd_copy_from)

add_test(NAME mon_handle_forward COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/mon-handle-forward.sh)
add_dependencies(check mon_handle_forward)

add_test(NAME osd_erasure_code_profile COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/osd-erasure-code-profile.sh)
add_dependencies(check osd_erasure_code_profile)

add_test(NAME osd_crush COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/osd-crush.sh)
add_dependencies(check osd_crush)

add_test(NAME mon_scrub COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/mon-scrub.sh)
add_dependencies(check mon_scrub)

add_test(NAME mon_ping COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/mon-ping.sh)
add_dependencies(check mon_ping)

add_test(NAME test_ceph_daemon COMMAND python ${CMAKE_SOURCE_DIR}/src/test/pybind/test_ceph_daemon.py)
add_dependencies(check test_ceph_daemon)

add_test(NAME rados_striper COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/libradosstriper/rados-striper.sh)
add_dependencies(check rados_striper)

add_test(NAME osd_bench COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/osd/osd-bench.sh)
add_dependencies(check osd_bench)

add_test(NAME osd_reactivate COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/osd/osd-reactivate.sh)
add_dependencies(check osd_reactivate)

add_test(NAME test_erasure_code COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/erasure-code/test-erasure-code.sh)
add_dependencies(check test_erasure_code)

add_test(NAME cephtool_test_mds COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/cephtool-test-mds.sh)
add_dependencies(check cephtool_test_mds)

add_test(NAME test_ceph_argparse_py COMMAND python ${CMAKE_SOURCE_DIR}/src/test/pybind/test_ceph_argparse.py)
add_dependencies(check test_ceph_argparse_py)

add_test(NAME test_erasure_eio COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/erasure-code/test-erasure-eio.sh)
add_dependencies(check test_erasure_eio)

add_test(NAME ceph_disk COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/ceph-disk.sh)
add_dependencies(check ceph_disk)

add_test(NAME readable COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/encoding/readable.sh)
add_dependencies(check readable)

add_test(NAME ceph_objectstore_tool COMMAND python ${CMAKE_SOURCE_DIR}/src/test/ceph_objectstore_tool.py)
add_dependencies(check ceph_objectstore_tool)

add_test(NAME test_ceph_helpers COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/test-ceph-helpers.sh)
add_dependencies(check test_ceph_helpers)

add_test(NAME cephtool_test_mon COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/cephtool-test-mon.sh)
add_dependencies(check cephtool_test_mon)

add_test(NAME encode_decode_non_regression COMMAND bash ${CMAKE_SOURCE_DIR}/qa/workunits/erasure-code/encode-decode-non-regression.sh)
add_dependencies(check encode_decode_non_regression)

add_test(NAME osd_scrub_repair COMMAND bash ${CMAKE_SOURCE_DIR}/src/osd/osd-scrub-repair.sh)
add_dependencies(check osd_scrub_repair)

set(UNITTEST_LIBS gtest_main ${PTHREAD_LIBS})
set(UNITTEST_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${CMAKE_SOURCE_DIR}/src/gtest/include -I${CMAKE_BINARY_DIR}/src/gtest/include -fno-strict-aliasing")

set(UNITTEST_LIBS gmock_main gmock gtest ${PTHREAD_LIBS})
set(UNITTEST_CXX_FLAGS "-I${CMAKE_SOURCE_DIR}/src/gmock/include -I${CMAKE_BINARY_DIR}/src/gmock/include -I${CMAKE_SOURCE_DIR}/src/gmock/gtest/include -I${CMAKE_BINARY_DIR}/src/gmock/gtest/include -fno-strict-aliasing")
  
# unittest_encoding
add_executable(unittest_encoding EXCLUDE_FROM_ALL
  encoding.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_encoding unittest_encoding)
add_dependencies(check unittest_encoding)
#target_link_libraries(unittest_encoding librados global boost_filesystem
target_link_libraries(unittest_encoding cephfs librados pthread rt m ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_encoding
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_addrs
add_executable(unittest_addrs EXCLUDE_FROM_ALL
  test_addrs.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_addrs unittest_addrs)
add_dependencies(check unittest_addrs)
target_link_libraries(unittest_addrs cephfs librados pthread rt m
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_addrs
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_blkdev
add_executable(unittest_blkdev EXCLUDE_FROM_ALL
  common/test_blkdev.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_blkdev unittest_blkdev)
add_dependencies(check unittest_blkdev)
target_link_libraries(unittest_blkdev
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_blkdev PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_bloom_filter
add_executable(unittest_bloom_filter EXCLUDE_FROM_ALL
  common/test_bloom_filter.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_bloom_filter unittest_bloom_filter)
add_dependencies(check unittest_bloom_filter)
target_link_libraries(unittest_bloom_filter global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_bloom_filter
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_histogram
add_executable(unittest_histogram EXCLUDE_FROM_ALL
  common/histogram.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_histogram unittest_histogram)
add_dependencies(check unittest_histogram)
target_link_libraries(unittest_histogram global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_histogram
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_prioritized_queue
add_executable(unittest_prioritized_queue EXCLUDE_FROM_ALL
  common/test_prioritized_queue.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_prioritized_queue unittest_prioritized_queue)
add_dependencies(check unittest_prioritized_queue)
target_link_libraries(unittest_prioritized_queue global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_prioritized_queue
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_str_map
add_executable(unittest_str_map EXCLUDE_FROM_ALL
  common/test_str_map.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_str_map unittest_str_map)
add_dependencies(check unittest_str_map)
target_link_libraries(unittest_str_map common global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS} common)
set_target_properties(unittest_str_map
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_sharedptr_registry
add_executable(unittest_sharedptr_registry EXCLUDE_FROM_ALL
  common/test_sharedptr_registry.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_sharedptr_registry unittest_sharedptr_registry)
add_dependencies(check unittest_sharedptr_registry)
target_link_libraries(unittest_sharedptr_registry global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_sharedptr_registry
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_shared_cache
add_executable(unittest_shared_cache EXCLUDE_FROM_ALL
  common/test_shared_cache.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_shared_cache unittest_shared_cache)
add_dependencies(check unittest_shared_cache)
target_link_libraries(unittest_shared_cache global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_shared_cache
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_sloppy_crc_map
add_executable(unittest_sloppy_crc_map EXCLUDE_FROM_ALL
  common/test_sloppy_crc_map.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_sloppy_crc_map unittest_sloppy_crc_map)
add_dependencies(check unittest_sloppy_crc_map)
target_link_libraries(unittest_sloppy_crc_map global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_sloppy_crc_map
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_time
set(unittest_time_srcs
  common/test_time.cc
  ${CMAKE_SOURCE_DIR}/src/common/ceph_time.cc
  )
add_executable(unittest_time
  ${unittest_time_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(unittest_time
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_time
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_util
add_executable(unittest_util EXCLUDE_FROM_ALL
  common/test_util.cc
  ${CMAKE_SOURCE_DIR}/src/common/util.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_util unittest_util)
add_dependencies(check unittest_util)
target_link_libraries(unittest_util
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_util
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_crush_wrapper
add_executable(unittest_crush_wrapper EXCLUDE_FROM_ALL
  crush/CrushWrapper.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_crush_wrapper unittest_crush_wrapper)
add_dependencies(check unittest_crush_wrapper)
target_link_libraries(unittest_crush_wrapper global crush ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_crush_wrapper PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_crush
add_executable(unittest_crush EXCLUDE_FROM_ALL
  crush/crush.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_crush unittest_crush)
add_dependencies(check unittest_crush)
target_link_libraries(unittest_crush global m ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS} ${EXTRALIBS})
set_target_properties(unittest_crush PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_osdmap
add_executable(unittest_osdmap EXCLUDE_FROM_ALL
  osd/TestOSDMap.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_osdmap unittest_osdmap)
add_dependencies(check unittest_osdmap)
target_link_libraries(unittest_osdmap global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_osdmap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_workqueue
add_executable(unittest_workqueue EXCLUDE_FROM_ALL
  test_workqueue.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_workqueue unittest_workqueue)
add_dependencies(check unittest_workqueue)
target_link_libraries(unittest_workqueue global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_workqueue PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})


# unittest_striper
add_executable(unittest_striper EXCLUDE_FROM_ALL
  test_striper.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_striper unittest_striper)
add_dependencies(check unittest_striper)
target_link_libraries(unittest_striper global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_striper PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_prebufferedstreambuf
add_executable(unittest_prebufferedstreambuf EXCLUDE_FROM_ALL
  test_prebufferedstreambuf.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_prebufferedstreambuf unittest_prebufferedstreambuf)
add_dependencies(check unittest_prebufferedstreambuf)
target_link_libraries(unittest_prebufferedstreambuf global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_prebufferedstreambuf PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_str_list
add_executable(unittest_str_list EXCLUDE_FROM_ALL
  test_str_list.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_str_list unittest_str_list)
add_dependencies(check unittest_str_list)
target_link_libraries(unittest_str_list global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_str_list PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_log
add_executable(unittest_log EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/log/test.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_log unittest_log)
add_dependencies(check unittest_log)
target_link_libraries(unittest_log global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_throttle
add_executable(unittest_throttle EXCLUDE_FROM_ALL
  common/Throttle.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_throttle unittest_throttle)
add_dependencies(check unittest_throttle)
target_link_libraries(unittest_throttle global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_throttle PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_base64
add_executable(unittest_base64 EXCLUDE_FROM_ALL
  base64.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_base64 unittest_base64)
add_dependencies(check unittest_base64)
target_link_libraries(unittest_base64 global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_base64 PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_ceph_argparse
add_executable(unittest_ceph_argparse EXCLUDE_FROM_ALL
  ceph_argparse.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_ceph_argparse unittest_ceph_argparse)
add_dependencies(check unittest_ceph_argparse)
target_link_libraries(unittest_ceph_argparse global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_argparse PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ceph_compatset
add_executable(unittest_ceph_compatset EXCLUDE_FROM_ALL
  ceph_compatset.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_ceph_compatset unittest_ceph_compatset)
add_dependencies(check unittest_ceph_compatset)
target_link_libraries(unittest_ceph_compatset global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_compatset PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mds_types
add_executable(unittest_mds_types EXCLUDE_FROM_ALL
  fs/mds_types.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_mds_types unittest_mds_types)
add_dependencies(check unittest_mds_types)
target_link_libraries(unittest_mds_types global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_mds_types PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_osd_types
add_executable(unittest_osd_types EXCLUDE_FROM_ALL
  osd/types.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_osd_types unittest_osd_types)
add_dependencies(check unittest_osd_types)
target_link_libraries(unittest_osd_types global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_osd_types PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_lru
add_executable(unittest_lru EXCLUDE_FROM_ALL
  common/test_lru.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_lru unittest_lru)
add_dependencies(check unittest_lru)
target_link_libraries(unittest_lru global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_lru PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_io_priority
add_executable(unittest_io_priority EXCLUDE_FROM_ALL
  common/test_io_priority.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_io_priority unittest_io_priority)
add_dependencies(check unittest_io_priority)
target_link_libraries(unittest_io_priority global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_io_priority PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_gather
add_executable(unittest_gather EXCLUDE_FROM_ALL
  gather.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_gather unittest_gather)
add_dependencies(check unittest_gather)
target_link_libraries(unittest_gather global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_gather PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# run_cmd
add_executable(unittest_run_cmd EXCLUDE_FROM_ALL
  run_cmd.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_run_cmd unittest_run_cmd)
add_dependencies(check unittest_run_cmd)
target_link_libraries(unittest_run_cmd global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_run_cmd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# signals
add_executable(unittest_signals EXCLUDE_FROM_ALL
  signals.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_signals unittest_signals)
add_dependencies(check unittest_signals)
target_link_libraries(unittest_signals global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} 
  ${UNITTEST_LIBS})
set_target_properties(unittest_signals PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_simple_spin
add_executable(unittest_simple_spin EXCLUDE_FROM_ALL
  simple_spin.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_simple_spin unittest_simple_spin)
add_dependencies(check unittest_simple_spin)
target_link_libraries(unittest_simple_spin global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_simple_spin PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_bufferlist
add_executable(unittest_bufferlist EXCLUDE_FROM_ALL
  bufferlist.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_bufferlist unittest_bufferlist)
add_dependencies(check unittest_bufferlist)
target_link_libraries(unittest_bufferlist global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_bufferlist PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_xlist
add_executable(unittest_xlist EXCLUDE_FROM_ALL
  test_xlist.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_xlist unittest_xlist)
add_dependencies(check unittest_xlist)
target_link_libraries(unittest_xlist common ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_xlist PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_librados
add_executable(unittest_librados EXCLUDE_FROM_ALL
  librados/librados.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_librados unittest_librados)
add_dependencies(check unittest_librados)
target_link_libraries(unittest_librados
  librados
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_librados PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_crc32c
add_executable(unittest_crc32c EXCLUDE_FROM_ALL
  common/test_crc32c.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_crc32c unittest_crc32c)
add_dependencies(check unittest_crc32c)
target_link_libraries(unittest_crc32c global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_crc32c PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_arch
add_executable(unittest_arch EXCLUDE_FROM_ALL
  test_arch.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_arch unittest_arch)
add_dependencies(check unittest_arch)
target_link_libraries(unittest_arch global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_arch PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_crypto
add_executable(unittest_crypto
  crypto.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_crypto unittest_crypto)
add_dependencies(check unittest_crypto)
target_link_libraries(unittest_crypto global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_crypto PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_crypto_init
add_executable(unittest_crypto_init EXCLUDE_FROM_ALL
  crypto_init.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_crypto_init unittest_crypto_init)
add_dependencies(check unittest_crypto_init)
target_link_libraries(unittest_crypto_init global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_crypto_init PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_perf_counters
add_executable(unittest_perf_counters EXCLUDE_FROM_ALL
  perf_counters.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_perf_counters unittest_perf_counters)
add_dependencies(check unittest_perf_counters)
target_link_libraries(unittest_perf_counters global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_perf_counters PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_admin_socket
add_executable(unittest_admin_socket EXCLUDE_FROM_ALL
  admin_socket.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_admin_socket unittest_admin_socket)
add_dependencies(check unittest_admin_socket)
target_link_libraries(unittest_admin_socket global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_admin_socket PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ceph_crypto
add_executable(unittest_ceph_crypto EXCLUDE_FROM_ALL
  ceph_crypto.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_ceph_crypto unittest_ceph_crypto)
add_dependencies(check unittest_ceph_crypto)
target_link_libraries(unittest_ceph_crypto global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_crypto PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_utf8
add_executable(unittest_utf8 EXCLUDE_FROM_ALL
  utf8.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_utf8 unittest_utf8)
add_dependencies(check unittest_utf8)
target_link_libraries(unittest_utf8 global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_utf8 PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mime
add_executable(unittest_mime EXCLUDE_FROM_ALL
  mime.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_mime unittest_mime)
add_dependencies(check unittest_mime)
target_link_libraries(unittest_mime global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_mime PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_escape
add_executable(unittest_escape EXCLUDE_FROM_ALL
  escape.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_escape unittest_escape)
add_dependencies(check unittest_escape)
target_link_libraries(unittest_escape global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_escape PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_strtol
add_executable(unittest_strtol EXCLUDE_FROM_ALL
  strtol.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_strtol unittest_strtol)
add_dependencies(check unittest_strtol)
target_link_libraries(unittest_strtol global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_strtol PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_confutils
add_executable(unittest_confutils EXCLUDE_FROM_ALL
  confutils.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_confutils unittest_confutils)
add_dependencies(check unittest_confutils)
target_link_libraries(unittest_confutils global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_confutils PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_config
add_executable(unittest_config EXCLUDE_FROM_ALL
  common/test_config.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_config unittest_config)
add_dependencies(check unittest_config)
target_link_libraries(unittest_config global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_context
add_executable(unittest_context EXCLUDE_FROM_ALL
  common/test_context.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_context unittest_context)
add_dependencies(check unittest_context)
target_link_libraries(unittest_context global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_context PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_chain_xattr
add_executable(unittest_chain_xattr EXCLUDE_FROM_ALL
  objectstore/chain_xattr.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(unittest_chain_xattr PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
add_test(unittest_chain_xattr unittest_chain_xattr)
add_dependencies(check unittest_chain_xattr)
target_link_libraries(unittest_chain_xattr
  os
  global
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_chain_xattr PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_safe_io
add_executable(unittest_safe_io EXCLUDE_FROM_ALL
  common/test_safe_io.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_safe_io unittest_safe_io)
add_dependencies(check unittest_safe_io)
target_link_libraries(unittest_safe_io global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_safe_io PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_heartbeatmap
add_executable(unittest_heartbeatmap EXCLUDE_FROM_ALL
  heartbeat_map.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_heartbeatmap unittest_heartbeatmap)
add_dependencies(check unittest_heartbeatmap)
target_link_libraries(unittest_heartbeatmap global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_heartbeatmap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

if(${WITH_RADOSGW})
  # unittest_formatter
  # why does this include rgw/rgw_formats.cc...?
  add_executable(unittest_formatter EXCLUDE_FROM_ALL
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_formats.cc
    $<TARGET_OBJECTS:heap_profiler_objs>
  )
  add_test(unittest_formatter unittest_formatter)
  add_dependencies(check unittest_formatter)
  target_link_libraries(unittest_formatter global ${CMAKE_DL_LIBS}
    ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
  set_target_properties(unittest_formatter PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
endif(${WITH_RADOSGW})

# unittest_daemon_config
add_executable(unittest_daemon_config EXCLUDE_FROM_ALL
  daemon_config.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_daemon_config unittest_daemon_config)
add_dependencies(check unittest_daemon_config)
target_link_libraries(unittest_daemon_config
  common
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${EXTRALIBS}
  )
set_target_properties(unittest_daemon_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_libcephfs_config
add_executable(unittest_libcephfs_config EXCLUDE_FROM_ALL
  libcephfs_config.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_libcephfs_config unittest_libcephfs_config)
add_dependencies(check unittest_libcephfs_config)
target_link_libraries(unittest_libcephfs_config cephfs ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_libcephfs_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_lfnindex
add_executable(unittest_lfnindex EXCLUDE_FROM_ALL
  os/TestLFNIndex.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_lfnindex unittest_lfnindex)
add_dependencies(check unittest_lfnindex)
target_link_libraries(unittest_lfnindex os global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_lfnindex PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_librados_config
set(unittest_librados_config_srcs librados/librados_config.cc)
add_executable(unittest_librados_config EXCLUDE_FROM_ALL
  ${unittest_librados_config_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_librados_config unittest_librados_config)
add_dependencies(check unittest_librados_config)
target_link_libraries(unittest_librados_config
  librados
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_librados_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_rbd_replay
add_executable(unittest_rbd_replay EXCLUDE_FROM_ALL
  test_rbd_replay.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  $<TARGET_OBJECTS:parse_secret_objs>
)
add_test(unittest_rbd_replay unittest_rbd_replay)
add_dependencies(check unittest_rbd_replay)
target_link_libraries(unittest_rbd_replay
  librbd
  librados
  global
  librbd_replay
  librbd_replay_ios
  keyutils
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS}
)
set_target_properties(unittest_rbd_replay PROPERTIES COMPILE_FLAGS
${UNITTEST_CXX_FLAGS})

# unittest_mon_moncap
add_executable(unittest_mon_moncap EXCLUDE_FROM_ALL
  ${unittest_mon_moncap_srcs}
  mon/moncap.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_mon_moncap unittest_mon_moncap)
add_dependencies(check unittest_mon_moncap)
target_link_libraries(unittest_mon_moncap mon global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_mon_moncap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mon_pgmap
add_executable(unittest_mon_pgmap EXCLUDE_FROM_ALL
  mon/PGMap.cc
  ${unittest_mon_moncap_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_mon_pgmap unittest_mon_pgmap)
add_dependencies(check unittest_mon_pgmap)
target_link_libraries(unittest_mon_pgmap mon global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_mon_pgmap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ecbackend
add_executable(unittest_ecbackend EXCLUDE_FROM_ALL
  osd/TestECBackend.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_ecbackend unittest_ecbackend)
add_dependencies(check unittest_ecbackend)
target_link_libraries(unittest_ecbackend osd global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ecbackend PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_osdscrub
add_executable(unittest_osdscrub EXCLUDE_FROM_ALL
  osd/TestOSDScrub.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_osdscrub unittest_osdscrub)
add_dependencies(check unittest_osdscrub)
target_link_libraries(unittest_osdscrub osd global dl os mon ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_osdscrub PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_pglog
add_executable(unittest_pglog EXCLUDE_FROM_ALL
  osd/TestPGLog.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_pglog unittest_pglog)
add_dependencies(check unittest_pglog)
target_link_libraries(unittest_pglog osd global dl ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_pglog PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_hitset
add_executable(unittest_hitset EXCLUDE_FROM_ALL
  osd/hitset.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_hitset unittest_hitset)
add_dependencies(check unittest_hitset)
target_link_libraries(unittest_hitset osd global ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_hitset PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_osd_osdcap
add_executable(unittest_osd_osdcap EXCLUDE_FROM_ALL
  osd/osdcap.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
)
add_test(unittest_osd_osdcap unittest_osd_osdcap)
add_dependencies(check unittest_osd_osdcap)
target_link_libraries(unittest_osd_osdcap osd global ${CMAKE_DL_LIBS}
${BLKID_LIBRARIES} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_osd_osdcap PROPERTIES COMPILE_FLAGS
${UNITTEST_CXX_FLAGS})

# unittest_snap_mapper
add_executable(unittest_snap_mapper EXCLUDE_FROM_ALL
  test_snap_mapper.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_snap_mapper unittest_snap_mapper)
add_dependencies(check unittest_snap_mapper)
target_link_libraries(unittest_snap_mapper osd global ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_snap_mapper PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

#TODO: rocksdb unittests
if(WITH_SLIBROCKSDB)
# unittest_rocksdb_option_static
add_executable(unittest_rocksdb_option_static EXCLUDE_FROM_ALL
  objectstore/TestRocksdbOptionParse.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_rocksdb_option_static unittest_rocksdb_option_static)
add_dependencies(check unittest_rocksdb_option_static)
target_link_libraries(unittest_rocksdb_option_static os librocksdb global ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set(UNITTEST_ROCKSDB_STATIC_FLAGS "-std=gnu++11 -Irocksdb/include")
set_target_properties(unittest_rocksdb_option_static PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS} ${LIBROCKSDB_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_ROCKSDB_STATIC_FLAGS})
endif(WITH_SLIBROCKSDB)

if(WITH_DLIBROCKSDB)
# unittest_rocksdb_option
add_executable(unittest_rocksdb_option EXCLUDE_FROM_ALL
  objectstore/TestRocksdbOptionParse.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_rocksdb_option_static unittest_rocksdb_option)
add_dependencies(check unittest_rocksdb_option)
target_link_libraries(unittest_rocksdb_option os rocksdb global ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set(UNITTEST_ROCKSDB_FLAGS "-std=gnu++11")
set_target_properties(unittest_rocksdb_option_static PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS} ${LIBROCKSDB_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_ROCKSDB_STATIC_FLAGS})
endif(WITH_DLIBROCKSDB)

# unittest_mds_authcap
add_executable(unittest_mds_authcap EXCLUDE_FROM_ALL
  mds/TestMDSAuthCaps.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_mds_authcap unittest_mds_authcap)
add_dependencies(check unittest_mds_authcap)
target_link_libraries(unittest_mds_authcap mds global ${CMAKE_DL_LIBS}
  ${BLKID_LIBRARIES} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_mds_authcap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mds_sessionfilter
add_executable(unittest_mds_sessionfilter EXCLUDE_FROM_ALL
    mds/TestSessionFilter.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  $<TARGET_OBJECTS:common_util_obj>
  )
add_test(unittest_mds_sessionfilter unittest_mds_sessionfilter)
add_dependencies(check unittest_mds_sessionfilter)
target_link_libraries(unittest_mds_sessionfilter mds osdc common global
  ${CMAKE_DL_LIBS} ${BLKID_LIBRARIES} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_mds_sessionfilter PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ipaddr
add_executable(unittest_ipaddr EXCLUDE_FROM_ALL
  test_ipaddr.cc)  
add_test(unittest_ipaddr unittest_ipaddr)
add_dependencies(check unittest_ipaddr)
target_link_libraries(unittest_ipaddr mon global ${CMAKE_DL_LIBS} 
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ipaddr PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_texttable
add_executable(unittest_texttable EXCLUDE_FROM_ALL
  test_texttable.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_texttable unittest_texttable)
add_dependencies(check unittest_texttable)
target_link_libraries(unittest_texttable mon global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_texttable PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_on_exit
add_executable(unittest_on_exit EXCLUDE_FROM_ALL
  on_exit.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_on_exit unittest_on_exit)
add_dependencies(check unittest_on_exit)
target_link_libraries(unittest_on_exit
  global
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
add_test(unittest_on_exit unittest_on_exit)
add_dependencies(check unittest_on_exit)
set_target_properties(unittest_on_exit PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_readahead
add_executable(unittest_readahead EXCLUDE_FROM_ALL
  common/Readahead.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_readahead unittest_readahead)
add_dependencies(check unittest_readahead)
target_link_libraries(unittest_readahead
  global
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
add_test(unittest_readahead unittest_readahead)
add_dependencies(check unittest_readahead)
set_target_properties(unittest_readahead PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_tableformatter
add_executable(unittest_tableformatter EXCLUDE_FROM_ALL
  common/test_tableformatter.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_tableformatter unittest_tableformatter)
add_dependencies(check unittest_tableformatter)
target_link_libraries(unittest_tableformatter
  global
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
add_test(unittest_tableformatter unittest_tableformatter)
add_dependencies(check unittest_tableformatter)
set_target_properties(unittest_tableformatter PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_bit_vector
add_executable(unittest_bit_vector EXCLUDE_FROM_ALL
  common/test_bit_vector.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(unittest_bit_vector unittest_bit_vector)
add_dependencies(check unittest_bit_vector)
target_link_libraries(unittest_bit_vector
  global
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
add_test(unittest_bit_vector unittest_bit_vector)
add_dependencies(check unittest_bit_vector)
set_target_properties(unittest_bit_vector PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_subprocess
add_executable(unittest_subprocess EXCLUDE_FROM_ALL
  test_subprocess.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_dependencies(check unittest_subprocess)
target_link_libraries(unittest_subprocess
  global
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_subprocess PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_pageset
add_executable(unittest_pageset EXCLUDE_FROM_ALL test_pageset.cc)
add_test(unittest_subprocess unittest_subprocess)
add_dependencies(check unittest_subprocess)
target_link_libraries(unittest_pageset ${UNITTEST_LIBS})
set_target_properties(unittest_pageset PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_async_compressor
add_executable(unittest_async_compressor EXCLUDE_FROM_ALL
  common/test_async_compressor.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
)
add_dependencies(check unittest_async_compressor)
target_link_libraries(unittest_async_compressor
  global
  compressor
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_async_compressor PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_subdirectory(erasure-code EXCLUDE_FROM_ALL)
#make check ends here

if(${WITH_RADOSGW})
  # test_cors
  set(test_cors_srcs test_cors.cc)
  add_executable(test_cors
    ${test_cors_srcs}
    $<TARGET_OBJECTS:heap_profiler_objs>
    )
  target_link_libraries(test_cors
    librados
    rgw_a
    global
    curl
    expat
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
  set_target_properties(test_cors PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_rgw_manifest
  set(test_rgw_manifest_srcs rgw/test_rgw_manifest.cc)
  add_executable(test_rgw_manifest
    ${test_rgw_manifest_srcs}
    $<TARGET_OBJECTS:heap_profiler_objs>
    )
  target_link_libraries(test_rgw_manifest
    rgw_a
    cls_rgw_client
    cls_lock_client
    cls_refcount_client
    cls_log_client
    cls_statelog_client
    cls_timeindex_client
    cls_version_client
    cls_replica_log_client
    cls_kvs
    cls_user_client
    librados
    global
    curl
    expat
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    ${TCMALLOC_LIBS}
    ${UNITTEST_LIBS}
    ${CRYPTO_LIBS}
    )
  set_target_properties(test_rgw_manifest PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_rgw_obj
  set(test_rgw_obj_srcs rgw/test_rgw_obj.cc)
  add_executable(test_rgw_obj
    ${test_rgw_obj_srcs}
    $<TARGET_OBJECTS:heap_profiler_objs>
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
    ${TCMALLOC_LIBS}
    ${UNITTEST_LIBS}
    ${CRYPTO_LIBS}
    )
  set_target_properties(test_rgw_obj PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_meta
  set(test_cls_rgw_meta_srcs test_rgw_admin_meta.cc)
  add_executable(test_cls_rgw_meta
    ${test_cls_rgw_meta_srcs}
    $<TARGET_OBJECTS:heap_profiler_objs>
    )
  target_link_libraries(test_cls_rgw_meta
    librados
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
    boost_regex
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS} ${CRYPTO_LIBS})
  set_target_properties(test_cls_rgw_meta PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_log
  set(test_cls_rgw_log_srcs
    test_rgw_admin_log.cc
    )
  add_executable(test_cls_rgw_log
    ${test_cls_rgw_log_srcs}
    $<TARGET_OBJECTS:heap_profiler_objs>
    )
  target_link_libraries(test_cls_rgw_log
    librados
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
    boost_regex
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    ${TCMALLOC_LIBS}
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
    $<TARGET_OBJECTS:heap_profiler_objs>
    )
  target_link_libraries(test_cls_rgw_opstate
    rgw_a
    librados
    cls_version_client
    cls_log_client
    cls_statelog_client
    cls_timeindex_client
    cls_refcount_client
    cls_rgw_client
    cls_user_client
    cls_lock_client
    global
    curl
    expat
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    ${TCMALLOC_LIBS}
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
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(multi_stress_watch librados global radostest
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# unittest_librbd
add_executable(unittest_librbd EXCLUDE_FROM_ALL
  librbd/test_librbd.cc
  librbd/test_fixture.cc
  librbd/test_ImageWatcher.cc
  librbd/test_internal.cc
  librbd/test_support.cc
  librbd/test_main.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  ${CMAKE_SOURCE_DIR}/src/common/secret.c
  )
add_dependencies(check unittest_librbd)
set_target_properties(unittest_librbd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_librbd
  librbd
  librados
  ${UNITTEST_LIBS}
  global
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  blkid
  udev
  keyutils
  radostest
  )

add_executable(test_librbd_fsx
  librbd/fsx.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  ${CMAKE_SOURCE_DIR}/src/common/secret.c
  )
target_link_libraries(test_librbd_fsx
  librbd
  librados
  global
  m
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  blkid
  udev
  keyutils
  )

add_executable(test_cls_rbd
  cls_rbd/test_cls_rbd.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  blkid
  udev
  keyutils
  )

add_executable(test_cls_refcount
  cls_refcount/test_cls_refcount.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_version
  cls_version/test_cls_version.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_log
  cls_log/test_cls_log.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )

add_executable(test_cls_statelog
  cls_statelog/test_cls_statelog.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_replica_log
  cls_replica_log/test_cls_replica_log.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_lock
  cls_lock/test_cls_lock.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_hello
  cls_hello/test_cls_hello.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_cls_hello PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_hello
  librados
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest
  ${UNITTEST_LIBS}
  )

if(${WITH_RADOSGW})
  add_executable(test_cls_rgw
    cls_rgw/test_cls_rgw.cc
    $<TARGET_OBJECTS:heap_profiler_objs>
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
    ${TCMALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    radostest)
endif(${WITH_RADOSGW})

add_executable(test_mon_workloadgen
  mon/test_mon_workloadgen.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_mon_workloadgen
  os
  osdc
  global
  ${EXTRALIBS}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_rados_api_cmd
  librados/cmd.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_cmd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_cmd
  librados global ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_io
  librados/io.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_io PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_io
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_c_write_operations
  librados/c_write_operations.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_c_write_operations PROPERTIES
  COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_c_write_operations
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_c_read_operations
  librados/c_read_operations.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_c_read_operations PROPERTIES COMPILE_FLAGS 
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_c_read_operations
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_aio
  librados/aio.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_aio PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_aio
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_list
  librados/list.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_list PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_list
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_nlist
  librados/nlist.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_nlist PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_nlist
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_pool
  librados/pool.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_pool PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS}
  )
target_link_libraries(test_rados_api_pool
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_stat
  librados/stat.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_stat PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_stat
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_watch_notify
  librados/watch_notify.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_watch_notify PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_watch_notify
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_cls
  librados/cls.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_cls PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_cls
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_misc
  librados/misc.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_misc PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_misc
  librados global ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

add_executable(test_rados_api_lock
  librados/lock.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_rados_api_lock PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_lock
  librados ${UNITTEST_LIBS} ${TCMALLOC_LIBS} radostest)

if(${WITH_CEPHFS})
  add_executable(test_libcephfs
    libcephfs/test.cc
    libcephfs/readdir_r_cb.cc
    libcephfs/caps.cc
    libcephfs/multiclient.cc
    libcephfs/flock.cc
    $<TARGET_OBJECTS:heap_profiler_objs>
  )
  set_target_properties(test_libcephfs PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(test_libcephfs
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${TCMALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    )
endif(${WITH_CEPHFS})  

add_executable(test_objectstore
  objectstore/store_test.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_keyvaluedb
  objectstore/test_kv.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_objectstore_workloadgen
  objectstore/workload_generator.cc
  objectstore/TestObjectStoreState.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_objectstore_workloadgen
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_filestore_idempotent
  objectstore/test_idempotent.cc
  objectstore/FileStoreTracker.cc
  common/ObjectContents.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_filestore_idempotent
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_filestore_idempotent_sequence
  objectstore/test_idempotent_sequence.cc
  objectstore/DeterministicOpSequence.cc
  objectstore/TestObjectStoreState.cc
  objectstore/FileStoreDiff.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_filestore_idempotent_sequence
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_xattr_bench
  xattr_bench.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_filejournal
  test_filejournal.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  )

add_executable(test_stress_watch
  test_stress_watch.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
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
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_objectcacher_stress
  osdc/object_cacher_stress.cc
  osdc/FakeWriteback.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_objectcacher_stress
  osdc
  global
  ${EXTRALIBS}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_object_map
  ObjectMap/test_object_map.cc
  ObjectMap/KeyValueDBMemory.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_object_map PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_object_map
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_keyvaluedb_atomicity
  ObjectMap/test_keyvaluedb_atomicity.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_keyvaluedb_atomicity PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_keyvaluedb_atomicity
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_keyvaluedb_iterators
  ObjectMap/test_keyvaluedb_iterators.cc
  ObjectMap/KeyValueDBMemory.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(test_keyvaluedb_iterators PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_keyvaluedb_iterators
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

if(${HAVE_FUSE})
  add_executable(test_cfuse_cache_invalidate
    test_cfuse_cache_invalidate.cc
    )
  target_link_libraries(test_cfuse_cache_invalidate
    global
    os
    ${EXTRALIBS}
    ${TCMALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    )
endif(${HAVE_FUSE})

if(${WITH_CEPHFS})
  add_executable(test_c_headers
    test_c_headers.c
    $<TARGET_OBJECTS:heap_profiler_objs>
  )
  target_link_libraries(test_c_headers
    librados
    cephfs
    ${EXTRALIBS}
    ${BLKID_LIBRARIES}
    ${TCMALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    )
endif(${WITH_CEPHFS})

add_executable(test_get_blkdev_size
  test_get_blkdev_size.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(test_get_blkdev_size
  common
  pthread
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(simple_server
  messenger/simple_server.cc
  messenger/simple_dispatcher.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(simple_server
  os global common boost_regex
  ${EXTRALIBS}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(simple_client
  messenger/simple_client.cc
  messenger/simple_dispatcher.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(simple_client
  os global common boost_regex
  ${EXTRALIBS}
  ${TCMALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

if(HAVE_XIO)
  add_executable(xio_server
    messenger/xio_server.cc
    messenger/xio_dispatcher.cc
    $<TARGET_OBJECTS:heap_profiler_objs>
    )
  target_link_libraries(xio_server
    os global common boost_regex
    ${Xio_LIBRARY} ibverbs rdmacm pthread rt
    ${EXTRALIBS}
    ${TCMALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    )

  add_executable(xio_client
    messenger/xio_client.cc
    messenger/xio_dispatcher.cc
    $<TARGET_OBJECTS:heap_profiler_objs>
    )
  target_link_libraries(xio_client
    os global common boost_regex
    ${Xio_LIBRARY} ibverbs rdmacm pthread rt
    ${EXTRALIBS}
    ${TCMALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    )
endif(HAVE_XIO)
