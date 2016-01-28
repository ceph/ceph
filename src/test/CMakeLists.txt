include(AddCephTest)

set(UNITTEST_LIBS gmock_main gmock gtest ${PTHREAD_LIBS} ${CMAKE_DL_LIBS})
set(UNITTEST_CXX_FLAGS "-I${CMAKE_SOURCE_DIR}/src/gmock/include -I${CMAKE_BINARY_DIR}/src/gmock/include -I${CMAKE_SOURCE_DIR}/src/gmock/gtest/include -I${CMAKE_BINARY_DIR}/src/gmock/gtest/include -fno-strict-aliasing")

# test_timers
add_executable(ceph_test_timers
  TestTimers.cc
  )
target_link_libraries(ceph_test_timers global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# test_signal_handlers
add_executable(ceph_test_signal_handlers
  TestSignalHandlers.cc
  )
target_link_libraries(ceph_test_signal_handlers global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# test_rewrite_latency
add_executable(ceph_test_rewrite_latency
  test_rewrite_latency.cc
  )
target_link_libraries(ceph_test_rewrite_latency common
  ${CMAKE_DL_LIBS}
  ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})

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

# bench_log
set(bench_log_srcs
  bench_log.cc
  )
add_executable(ceph_bench_log
  ${bench_log_srcs}
  )
target_link_libraries(ceph_bench_log global pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# ceph_test_mutate
add_executable(ceph_test_mutate
  test_mutate.cc
  )
target_link_libraries(ceph_test_mutate global librados ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS})

# test_trans
add_executable(test_trans
  test_trans.cc
  )
target_link_libraries(test_trans os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

## Benchmarks

# smalliobench
set(smalliobench_srcs
  bench/small_io_bench.cc
  bench/rados_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(ceph_smalliobench
  ${smalliobench_srcs}
  )
target_link_libraries(ceph_smalliobench librados ${Boost_PROGRAM_OPTIONS_LIBRARY} global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# ceph_smalliobenchrbd
if (${WITH_RBD})
  set(ceph_smalliobenchrbd_srcs
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

# ceph_omapbench
set(omapbench_srcs
  omap_bench.cc
  )
add_executable(ceph_omapbench
  ${omapbench_srcs}
  )
target_link_libraries(ceph_omapbench
  librados
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

# ceph_kvstorebench
set(kvstorebench_srcs
  kv_store_bench.cc
  ${CMAKE_SOURCE_DIR}/src/key_value_store/kv_flat_btree_async.cc
  )
add_executable(ceph_kvstorebench
  ${kvstorebench_srcs}
  )
target_link_libraries(ceph_kvstorebench librados global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

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

if(${WITH_RADOSGW})
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

# ceph_multi_stress_watch
add_executable(ceph_multi_stress_watch
  multi_stress_watch.cc
  )
target_link_libraries(ceph_multi_stress_watch librados global radostest
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

add_executable(ceph_test_cls_rbd
  cls_rbd/test_cls_rbd.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  ${CMAKE_SOURCE_DIR}/src/common/secret.c
  )
set_target_properties(ceph_test_cls_rbd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_rbd
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

add_executable(ceph_test_cls_refcount
  cls_refcount/test_cls_refcount.cc
  )
set_target_properties(ceph_test_cls_refcount PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_refcount
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

add_executable(ceph_test_cls_version
  cls_version/test_cls_version.cc
  )
set_target_properties(ceph_test_cls_version PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_version
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

add_executable(ceph_test_cls_log
  cls_log/test_cls_log.cc
  )
set_target_properties(ceph_test_cls_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_log
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

add_executable(ceph_test_cls_statelog
  cls_statelog/test_cls_statelog.cc
  )
set_target_properties(ceph_test_cls_statelog PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_statelog
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

add_executable(ceph_test_cls_replica_log
  cls_replica_log/test_cls_replica_log.cc
  )
set_target_properties(ceph_test_cls_replica_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_replica_log
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

add_executable(ceph_test_cls_lock
  cls_lock/test_cls_lock.cc
  )
set_target_properties(ceph_test_cls_lock PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_lock
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

add_executable(ceph_test_cls_hello
  cls_hello/test_cls_hello.cc
  )
set_target_properties(ceph_test_cls_hello PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_hello
  librados
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest
  ${UNITTEST_LIBS}
  )

add_executable(ceph_test_cls_numops cls_numops/test_cls_numops.cc)
set_target_properties(ceph_test_cls_numops PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_numops 
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
  add_executable(ceph_test_cls_rgw
    cls_rgw/test_cls_rgw.cc
    )
  set_target_properties(ceph_test_cls_rgw PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(ceph_test_cls_rgw
    cls_rgw_client
    librados
    global
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    radostest)
endif(${WITH_RADOSGW})

# ceph_test_async_driver
add_executable(ceph_test_async_driver
  msgr/test_async_driver.cc
  )
set_target_properties(ceph_test_async_driver PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_async_driver os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

# ceph_test_msgr
add_executable(ceph_test_msgr
  msgr/test_msgr.cc
  )
set_target_properties(ceph_test_msgr PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_msgr os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

#ceph_perf_local
add_executable(ceph_perf_local 
  perf_local.cc
  perf_helper.cc)
#INTEL_SSE & INTEL_SSE2 flags
set(PERF_LOCAL_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse2") 
set_target_properties(ceph_perf_local PROPERTIES COMPILE_FLAGS
  ${PERF_LOCAL_FLAGS})
target_link_libraries(ceph_perf_local os global ${UNITTEST_LIBS})

#ceph_perf_objectstore
add_executable(ceph_perf_objectstore objectstore/ObjectStoreTransactionBenchmark.cc)
set_target_properties(ceph_perf_objectstore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_perf_objectstore os osdc global ${UNITTEST_LIBS})

#ceph_perf_msgr_server
add_executable(ceph_perf_msgr_server msgr/perf_msgr_server.cc)
set_target_properties(ceph_perf_msgr_server PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_perf_msgr_server os global ${UNITTEST_LIBS})

#ceph_perf_msgr_client
add_executable(ceph_perf_msgr_client msgr/perf_msgr_client.cc)
set_target_properties(ceph_perf_msgr_client PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_perf_msgr_client os global ${UNITTEST_LIBS})

#ceph_test_objectstore
add_executable(ceph_test_objectstore
  objectstore/store_test.cc
  )
set_target_properties(ceph_test_objectstore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_objectstore
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

#ceph_test_keyvaluedb
add_executable(ceph_test_keyvaluedb
  objectstore/test_kv.cc
  )
set_target_properties(ceph_test_keyvaluedb PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_keyvaluedb
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

#ceph_test_filestore
add_executable(ceph_test_filestore filestore/TestFileStore.cc)
set_target_properties(ceph_test_filestore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_filestore os global ${UNITTEST_LIBS})

# ceph_test_objectstore_workloadgen
add_executable(ceph_test_objectstore_workloadgen
  objectstore/workload_generator.cc
  objectstore/TestObjectStoreState.cc
  )
target_link_libraries(ceph_test_objectstore_workloadgen
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_filestore_idempotent
add_executable(ceph_test_filestore_idempotent
  objectstore/test_idempotent.cc
  objectstore/FileStoreTracker.cc
  common/ObjectContents.cc
  )
target_link_libraries(ceph_test_filestore_idempotent
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_filestore_idempotent_sequence
add_executable(ceph_test_filestore_idempotent_sequence
  objectstore/test_idempotent_sequence.cc
  objectstore/DeterministicOpSequence.cc
  objectstore/TestObjectStoreState.cc
  objectstore/FileStoreDiff.cc
  )
target_link_libraries(ceph_test_filestore_idempotent_sequence
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_xattr_bench
add_executable(ceph_test_xattr_bench
  xattr_bench.cc
  )
set_target_properties(ceph_test_xattr_bench PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_xattr_bench
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_filejournal
add_executable(ceph_test_filejournal
  test_filejournal.cc
  )
set_target_properties(ceph_test_filejournal PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_filejournal
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_object_map
add_executable(ceph_test_object_map
  ObjectMap/test_object_map.cc
  ObjectMap/KeyValueDBMemory.cc
  )
set_target_properties(ceph_test_object_map PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_object_map
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_keyvaluedb_atomicity
add_executable(ceph_test_keyvaluedb_atomicity
  ObjectMap/test_keyvaluedb_atomicity.cc
  )
set_target_properties(ceph_test_keyvaluedb_atomicity PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_keyvaluedb_atomicity
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_keyvaluedb_iterators
add_executable(ceph_test_keyvaluedb_iterators
  ObjectMap/test_keyvaluedb_iterators.cc
  ObjectMap/KeyValueDBMemory.cc
  )
set_target_properties(ceph_test_keyvaluedb_iterators PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_keyvaluedb_iterators
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

# ceph_smalliobenchfs
set(ceph_smalliobenchfs_srcs
  bench/small_io_bench_fs.cc
  bench/testfilestore_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(ceph_smalliobenchfs
  ${ceph_smalliobenchfs_srcs}
  )
target_link_libraries(ceph_smalliobenchfs librados ${Boost_PROGRAM_OPTIONS_LIBRARY} os global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# ceph_smalliobenchdumb
set(smalliobenchdumb_srcs
  bench/small_io_bench_dumb.cc
  bench/dumb_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(ceph_smalliobenchdumb
  ${smalliobenchdumb_srcs}
  )
target_link_libraries(ceph_smalliobenchdumb librados ${Boost_PROGRAM_OPTIONS_LIBRARY} os global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# ceph_test_keys
add_executable(ceph_test_keys
  testkeys.cc
  )
target_link_libraries(ceph_test_keys mon global ${CMAKE_DL_LIBS})

# ceph_tpbench
set(tpbench_srcs
  bench/tp_bench.cc
  bench/detailed_stat_collector.cc)
add_executable(ceph_tpbench
  ${tpbench_srcs}
  )
target_link_libraries(ceph_tpbench librados ${Boost_PROGRAM_OPTIONS_LIBRARY} global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

# ceph_test_snap_mapper
add_executable(ceph_test_snap_mapper EXCLUDE_FROM_ALL
  test_snap_mapper.cc
  )
target_link_libraries(ceph_test_snap_mapper osd global ${BLKID_LIBRARIES})

add_executable(ceph_test_rados_api_cmd
  librados/cmd.cc
  )
set_target_properties(ceph_test_rados_api_cmd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_cmd
  librados global ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_io
  librados/io.cc
  )
set_target_properties(ceph_test_rados_api_io PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_io
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_c_write_operations
  librados/c_write_operations.cc
  )
set_target_properties(ceph_test_rados_api_c_write_operations PROPERTIES
  COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_c_write_operations
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_c_read_operations
  librados/c_read_operations.cc
  )
set_target_properties(ceph_test_rados_api_c_read_operations PROPERTIES COMPILE_FLAGS 
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_c_read_operations
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_aio
  librados/aio.cc
  )
set_target_properties(ceph_test_rados_api_aio PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_aio
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_list
  librados/list.cc
  )
set_target_properties(ceph_test_rados_api_list PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_list
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_nlist
  librados/nlist.cc
  )
set_target_properties(ceph_test_rados_api_nlist PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_nlist
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_pool
  librados/pool.cc
  )
set_target_properties(ceph_test_rados_api_pool PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS}
  )
target_link_libraries(ceph_test_rados_api_pool
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_stat
  librados/stat.cc
  )
set_target_properties(ceph_test_rados_api_stat PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_stat
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_watch_notify
  librados/watch_notify.cc
  )
set_target_properties(ceph_test_rados_api_watch_notify PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_watch_notify
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_cls
  librados/cls.cc
  )
set_target_properties(ceph_test_rados_api_cls PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_cls
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_misc
  librados/misc.cc
  )
set_target_properties(ceph_test_rados_api_misc PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_misc
  librados global ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_lock
  librados/lock.cc
  )
set_target_properties(ceph_test_rados_api_lock PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_lock
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_tier
  librados/tier.cc
  )
set_target_properties(ceph_test_rados_api_tier PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_tier
  global librados ${UNITTEST_LIBS} ${Boost_SYSTEM_LIBRARY} radostest)

add_executable(ceph_test_rados_api_snapshots
  librados/snapshots.cc
  )
set_target_properties(ceph_test_rados_api_snapshots PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_snapshots
  librados ${UNITTEST_LIBS} radostest)

add_library(rados_striper_test STATIC libradosstriper/TestCase.cc)
target_link_libraries(rados_striper_test radostest)
set_target_properties(rados_striper_test PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_executable(ceph_test_rados_api_striping
  libradosstriper/striping.cc
  )
target_link_libraries(ceph_test_rados_api_striping librados libradosstriper 
  ${UNITTEST_LIBS} rados_striper_test)
set_target_properties(ceph_test_rados_api_striping PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

if(${WITH_CEPHFS})
  add_executable(ceph_test_libcephfs
    libcephfs/test.cc
    libcephfs/readdir_r_cb.cc
    libcephfs/caps.cc
    libcephfs/multiclient.cc
    libcephfs/flock.cc
    libcephfs/acl.cc
    libcephfs/access.cc
  )
  set_target_properties(ceph_test_libcephfs PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(ceph_test_libcephfs
    cephfs
    librados
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
endif(${WITH_CEPHFS})  

# unittest_transaction
add_executable(unittest_transaction EXCLUDE_FROM_ALL 
  objectstore/test_transaction.cc)
add_ceph_unittest(unittest_transaction ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_transaction)
target_link_libraries(unittest_transaction os common)

add_executable(ceph_test_stress_watch
  test_stress_watch.cc
  )
set_target_properties(ceph_test_stress_watch PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_stress_watch
  librados
  global
  ${UNITTEST_LIBS}
  radostest
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

add_executable(ceph_test_objectcacher_stress
  osdc/object_cacher_stress.cc
  osdc/FakeWriteback.cc
  )
target_link_libraries(ceph_test_objectcacher_stress
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )

if(${HAVE_FUSE})
  add_executable(ceph_test_cfuse_cache_invalidate
    test_cfuse_cache_invalidate.cc
    )
  target_link_libraries(ceph_test_cfuse_cache_invalidate
    global
    os
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
endif(${HAVE_FUSE})

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

add_executable(ceph_test_get_blkdev_size
  test_get_blkdev_size.cc
  )
target_link_libraries(ceph_test_get_blkdev_size
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

#make check starts here

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
  ceph-objectstore-tool
  ceph-monstore-tool
  osdmaptool
  rbd
  radosgw-admin)

add_ceph_test(test-ceph-helpers.sh ${CMAKE_CURRENT_SOURCE_DIR}/test-ceph-helpers.sh)
add_ceph_test(erasure-decode-non-regression.sh ${CMAKE_SOURCE_DIR}/qa/workunits/erasure-code/encode-decode-non-regression.sh)

add_ceph_test(ceph_objectstore_tool.py ${CMAKE_CURRENT_SOURCE_DIR}/ceph_objectstore_tool.py)
add_ceph_test(cephtool-test-mds.sh ${CMAKE_CURRENT_SOURCE_DIR}/cephtool-test-mds.sh)
add_ceph_test(cephtool-test-mon.sh ${CMAKE_CURRENT_SOURCE_DIR}/cephtool-test-mon.sh)
add_ceph_test(cephtool-test-osd.sh ${CMAKE_CURRENT_SOURCE_DIR}/cephtool-test-osd.sh)
add_ceph_test(cephtool-test-rados.sh ${CMAKE_CURRENT_SOURCE_DIR}/cephtool-test-rados.sh)
add_ceph_test(run-rbd-unit-tests.sh ${CMAKE_CURRENT_SOURCE_DIR}/run-rbd-unit-tests.sh)
add_ceph_test(run-cli-tests ${CMAKE_CURRENT_SOURCE_DIR}/run-cli-tests)
add_ceph_test(test_objectstore_memstore.sh ${CMAKE_CURRENT_SOURCE_DIR}/test_objectstore_memstore.sh)
add_ceph_test(test_pidfile.sh ${CMAKE_CURRENT_SOURCE_DIR}/test_pidfile.sh)
add_ceph_test(unittest_bufferlist.sh ${CMAKE_SOURCE_DIR}/src/unittest_bufferlist.sh)

add_test(NAME run-tox-ceph-disk COMMAND bash ${CMAKE_SOURCE_DIR}/src/ceph-disk/run-tox.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check run-tox-ceph-disk)

add_test(NAME run-tox-ceph-detect-init COMMAND bash ${CMAKE_SOURCE_DIR}/src/ceph-detect-init/run-tox.sh WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src)
add_dependencies(check run-tox-ceph-disk)

add_subdirectory(common)
add_subdirectory(encoding)

# erasure-code directory
add_ceph_test(test-erasure-code.sh ${CMAKE_SOURCE_DIR}/src/test/erasure-code/test-erasure-code.sh)
add_ceph_test(test-erasure-eio.sh ${CMAKE_SOURCE_DIR}/src/test/erasure-code/test-erasure-eio.sh)

# fs directory

# unittest_mds_types
add_executable(unittest_mds_types EXCLUDE_FROM_ALL
  fs/mds_types.cc
  )
add_ceph_unittest(unittest_mds_types ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mds_types)
target_link_libraries(unittest_mds_types global)

#libradosstriper directory
add_ceph_test(rados-striper.sh ${CMAKE_SOURCE_DIR}/src/test/libradosstriper/rados-striper.sh)


#libradosdirectory
# unittest_librados
add_executable(unittest_librados EXCLUDE_FROM_ALL
  librados/librados.cc
  )
add_ceph_unittest(unittest_librados ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_librados)
target_link_libraries(unittest_librados librados global ${BLKID_LIBRARIES})

# unittest_librados_config
set(unittest_librados_config_srcs librados/librados_config.cc)
add_executable(unittest_librados_config EXCLUDE_FROM_ALL
  ${unittest_librados_config_srcs}
  )
add_ceph_unittest(unittest_librados_config ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_librados_config)
target_link_libraries(unittest_librados_config
  librados
  global
  ${BLKID_LIBRARIES}
  )

#mds directory
# unittest_mds_authcap
add_executable(unittest_mds_authcap EXCLUDE_FROM_ALL
  mds/TestMDSAuthCaps.cc
  )
add_ceph_unittest(unittest_mds_authcap ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mds_authcap)
target_link_libraries(unittest_mds_authcap mds global ${BLKID_LIBRARIES})

# unittest_mds_sessionfilter
add_executable(unittest_mds_sessionfilter EXCLUDE_FROM_ALL
    mds/TestSessionFilter.cc
  $<TARGET_OBJECTS:common_util_obj>
  )
add_ceph_unittest(unittest_mds_sessionfilter ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mds_sessionfilter)
target_link_libraries(unittest_mds_sessionfilter mds osdc common global ${BLKID_LIBRARIES})

add_subdirectory(mon)

#objectstore directory

# unittest_chain_xattr
add_executable(unittest_chain_xattr EXCLUDE_FROM_ALL
  objectstore/chain_xattr.cc
  )
add_ceph_unittest(unittest_chain_xattr ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_chain_xattr)
target_link_libraries(unittest_chain_xattr os global)

# unittest_rocksdb_option
add_executable(unittest_rocksdb_option EXCLUDE_FROM_ALL
  objectstore/TestRocksdbOptionParse.cc
  )
add_ceph_unittest(unittest_rocksdb_option ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_rocksdb_option)
target_link_libraries(unittest_rocksdb_option global os ${BLKID_LIBRARIES})

# unittest_bluefs
add_executable(unittest_bluefs EXCLUDE_FROM_ALL objectstore/test_bluefs.cc)
add_ceph_unittest(unittest_bluefs ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_bluefs)
target_link_libraries(unittest_bluefs os global)

# unittest_bluestore_types
add_executable(unittest_bluestore_types EXCLUDE_FROM_ALL objectstore/test_bluestore_types.cc)
add_ceph_unittest(unittest_bluestore_types ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_bluestore_types)
target_link_libraries(unittest_bluestore_types os global)
  

#os directory
# unittest_lfnindex
add_executable(unittest_lfnindex EXCLUDE_FROM_ALL
  os/TestLFNIndex.cc
  )
add_ceph_unittest(unittest_lfnindex ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_lfnindex)
target_link_libraries(unittest_lfnindex os global)

add_subdirectory(osd)

#pybind
add_ceph_test(test_ceph_daemon.py ${CMAKE_SOURCE_DIR}/src/test/pybind/test_ceph_daemon.py)
add_ceph_test(test_ceph_argparse.py ${CMAKE_SOURCE_DIR}/src/test/pybind/test_ceph_argparse.py)

# unittest_admin_socket
add_executable(unittest_admin_socket EXCLUDE_FROM_ALL
  admin_socket.cc
  )
add_ceph_unittest(unittest_admin_socket ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_admin_socket)
target_link_libraries(unittest_admin_socket global) 

# unittest_encoding
add_executable(unittest_encoding EXCLUDE_FROM_ALL
  encoding.cc
  )
add_ceph_unittest(unittest_encoding ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_encoding)
target_link_libraries(unittest_encoding librados pthread rt m ${BLKID_LIBRARIES})

# unittest_addrs
add_executable(unittest_addrs EXCLUDE_FROM_ALL
  test_addrs.cc
  )
add_ceph_unittest(unittest_addrs ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_addrs)
target_link_libraries(unittest_addrs librados pthread rt m ${BLKID_LIBRARIES}) 

# unittest_workqueue
add_executable(unittest_workqueue EXCLUDE_FROM_ALL
  test_workqueue.cc
  )
add_ceph_unittest(unittest_workqueue ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_workqueue)
target_link_libraries(unittest_workqueue global ${BLKID_LIBRARIES})

# unittest_striper
add_executable(unittest_striper EXCLUDE_FROM_ALL
  test_striper.cc
  )
add_ceph_unittest(unittest_striper ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_striper)
target_link_libraries(unittest_striper global ${BLKID_LIBRARIES})

# unittest_prebufferedstreambuf
add_executable(unittest_prebufferedstreambuf EXCLUDE_FROM_ALL
  test_prebufferedstreambuf.cc
  )
add_ceph_unittest(unittest_prebufferedstreambuf ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_prebufferedstreambuf)
target_link_libraries(unittest_prebufferedstreambuf global ${BLKID_LIBRARIES})

# unittest_str_list
add_executable(unittest_str_list EXCLUDE_FROM_ALL
  test_str_list.cc
  )
add_ceph_unittest(unittest_str_list ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_str_list)
target_link_libraries(unittest_str_list global ${BLKID_LIBRARIES})

# unittest_log
add_executable(unittest_log EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/log/test.cc
  )
add_ceph_unittest(unittest_log ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_log)
target_link_libraries(unittest_log global)

# unittest_base64
add_executable(unittest_base64 EXCLUDE_FROM_ALL
  base64.cc
  )
add_ceph_unittest(unittest_base64 ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_base64)
target_link_libraries(unittest_base64 global) 

# unittest_ceph_argparse
add_executable(unittest_ceph_argparse EXCLUDE_FROM_ALL
  ceph_argparse.cc
  )
add_ceph_unittest(unittest_ceph_argparse ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_ceph_argparse)
target_link_libraries(unittest_ceph_argparse global)

# unittest_ceph_compatset
add_executable(unittest_ceph_compatset EXCLUDE_FROM_ALL
  ceph_compatset.cc
  )
add_ceph_unittest(unittest_ceph_compatset ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_ceph_compatset)
target_link_libraries(unittest_ceph_compatset global)

# unittest_gather
add_executable(unittest_gather EXCLUDE_FROM_ALL
  gather.cc
  )
add_ceph_unittest(unittest_gather ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_gather)
target_link_libraries(unittest_gather global)

# unittest_run_cmd
add_executable(unittest_run_cmd EXCLUDE_FROM_ALL
  run_cmd.cc
  )
add_ceph_unittest(unittest_run_cmd ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_run_cmd)
target_link_libraries(unittest_run_cmd global)

# signals
add_executable(unittest_signals EXCLUDE_FROM_ALL
  signals.cc
  )
add_ceph_unittest(unittest_signals ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_signals)
target_link_libraries(unittest_signals global)

# unittest_simple_spin
add_executable(unittest_simple_spin EXCLUDE_FROM_ALL
  simple_spin.cc
  )
add_ceph_unittest(unittest_simple_spin ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_simple_spin)
target_link_libraries(unittest_simple_spin global)

# unittest_bufferlist
add_executable(unittest_bufferlist EXCLUDE_FROM_ALL
  bufferlist.cc
  )
add_ceph_unittest(unittest_bufferlist ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_bufferlist)
target_link_libraries(unittest_bufferlist global)

# unittest_xlist
add_executable(unittest_xlist EXCLUDE_FROM_ALL
  test_xlist.cc
  )
add_ceph_unittest(unittest_xlist ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_xlist)
target_link_libraries(unittest_xlist common)

# unittest_arch
add_executable(unittest_arch EXCLUDE_FROM_ALL
  test_arch.cc
  )
add_ceph_unittest(unittest_arch ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_arch)
target_link_libraries(unittest_arch global)

# unittest_crypto
add_executable(unittest_crypto EXCLUDE_FROM_ALL
  crypto.cc
  )
add_ceph_unittest(unittest_crypto ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_crypto)
target_link_libraries(unittest_crypto global)

# unittest_crypto_init
add_executable(unittest_crypto_init EXCLUDE_FROM_ALL
  crypto_init.cc
  )
add_ceph_unittest(unittest_crypto_init ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_crypto_init)
target_link_libraries(unittest_crypto_init global)

# unittest_perf_counters
add_executable(unittest_perf_counters EXCLUDE_FROM_ALL
  perf_counters.cc
  )
add_ceph_unittest(unittest_perf_counters ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_perf_counters)
target_link_libraries(unittest_perf_counters global)

# unittest_ceph_crypto
add_executable(unittest_ceph_crypto EXCLUDE_FROM_ALL
  ceph_crypto.cc
  )
add_ceph_unittest(unittest_ceph_crypto ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_ceph_crypto)
target_link_libraries(unittest_ceph_crypto global)

# unittest_utf8
add_executable(unittest_utf8 EXCLUDE_FROM_ALL
  utf8.cc
  )
add_ceph_unittest(unittest_utf8 ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_utf8)
target_link_libraries(unittest_utf8 global)

# unittest_mime
add_executable(unittest_mime EXCLUDE_FROM_ALL
  mime.cc
  )
add_ceph_unittest(unittest_mime ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_mime)
target_link_libraries(unittest_mime global)

# unittest_escape
add_executable(unittest_escape EXCLUDE_FROM_ALL
  escape.cc
  )
add_ceph_unittest(unittest_escape ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_escape)
target_link_libraries(unittest_escape global)

# unittest_strtol
add_executable(unittest_strtol EXCLUDE_FROM_ALL
  strtol.cc
  )
add_ceph_unittest(unittest_strtol ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_strtol)
target_link_libraries(unittest_strtol global)

# unittest_confutils
add_executable(unittest_confutils EXCLUDE_FROM_ALL
  confutils.cc
  )
add_ceph_unittest(unittest_confutils ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_confutils)
target_link_libraries(unittest_confutils global)

# unittest_heartbeatmap
add_executable(unittest_heartbeatmap EXCLUDE_FROM_ALL
  heartbeat_map.cc
  )
add_ceph_unittest(unittest_heartbeatmap ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_heartbeatmap)
target_link_libraries(unittest_heartbeatmap global)

if(${WITH_RADOSGW})
  # unittest_formatter
  # why does this include rgw/rgw_formats.cc...?
  add_executable(unittest_formatter EXCLUDE_FROM_ALL
    formatter.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_formats.cc
  )
  add_ceph_unittest(unittest_formatter ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_formatter)
  target_link_libraries(unittest_formatter global)
endif(${WITH_RADOSGW})

# unittest_daemon_config
add_executable(unittest_daemon_config EXCLUDE_FROM_ALL
  daemon_config.cc
  )
add_ceph_unittest(unittest_daemon_config ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_daemon_config)
target_link_libraries(unittest_daemon_config
  common
  global
  ${BLKID_LIBRARIES}
  ${EXTRALIBS}
  )

# unittest_libcephfs_config
add_executable(unittest_libcephfs_config EXCLUDE_FROM_ALL
  libcephfs_config.cc
  )
add_ceph_unittest(unittest_libcephfs_config ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_libcephfs_config)
target_link_libraries(unittest_libcephfs_config cephfs)

# unittest_rbd_replay
add_executable(unittest_rbd_replay EXCLUDE_FROM_ALL
  test_rbd_replay.cc
  $<TARGET_OBJECTS:parse_secret_objs>
)
add_ceph_unittest(unittest_rbd_replay ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_rbd_replay)
target_link_libraries(unittest_rbd_replay
  librbd
  librados
  global
  rbd_replay
  rbd_replay_ios
  keyutils
  ${BLKID_LIBRARIES}
  )

# unittest_ipaddr
add_executable(unittest_ipaddr EXCLUDE_FROM_ALL
  test_ipaddr.cc)  
add_ceph_unittest(unittest_ipaddr ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_ipaddr)
target_link_libraries(unittest_ipaddr mon global)

# unittest_texttable
add_executable(unittest_texttable EXCLUDE_FROM_ALL
  test_texttable.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  )
add_ceph_unittest(unittest_texttable ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_texttable)
target_link_libraries(unittest_texttable mon global)

# unittest_on_exit
add_executable(unittest_on_exit EXCLUDE_FROM_ALL
  on_exit.cc
  )
add_ceph_unittest(unittest_on_exit ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_on_exit)
target_link_libraries(unittest_on_exit global)

# unittest_subprocess
add_executable(unittest_subprocess EXCLUDE_FROM_ALL
  test_subprocess.cc
  )
add_ceph_unittest(unittest_subprocess ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_subprocess)
target_link_libraries(unittest_subprocess global)

# unittest_pageset
add_executable(unittest_pageset EXCLUDE_FROM_ALL test_pageset.cc)
add_ceph_unittest(unittest_pageset ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_pageset)

add_subdirectory(erasure-code EXCLUDE_FROM_ALL)

#make check ends here

