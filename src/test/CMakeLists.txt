include(AddCephTest)

set(UNITTEST_LIBS gmock_main gmock gtest ${PTHREAD_LIBS} ${CMAKE_DL_LIBS})
set(UNITTEST_CXX_FLAGS "-I${CMAKE_SOURCE_DIR}/src/gmock/include -I${CMAKE_BINARY_DIR}/src/gmock/include -I${CMAKE_SOURCE_DIR}/src/gmock/gtest/include -I${CMAKE_BINARY_DIR}/src/gmock/gtest/include -fno-strict-aliasing")

add_subdirectory(bench)
add_subdirectory(cls_hello)
add_subdirectory(cls_lock)
add_subdirectory(cls_log)
add_subdirectory(cls_numops)
add_subdirectory(cls_rbd)
add_subdirectory(cls_refcount)
add_subdirectory(cls_replica_log)
add_subdirectory(cls_rgw)
add_subdirectory(cls_statelog)
add_subdirectory(cls_version)
add_subdirectory(common)
add_subdirectory(compressor)
add_subdirectory(crush)
add_subdirectory(encoding)
add_subdirectory(erasure-code)
add_subdirectory(filestore)
add_subdirectory(fs)
add_subdirectory(journal)
add_subdirectory(libcephfs)
add_subdirectory(librados)
add_subdirectory(librados_test_stub)
add_subdirectory(libradosstriper)
add_subdirectory(librbd)
add_subdirectory(messenger)
add_subdirectory(mds)
add_subdirectory(mon)
add_subdirectory(msgr)
add_subdirectory(ObjectMap)
add_subdirectory(objectstore)
add_subdirectory(os)
add_subdirectory(osd)
add_subdirectory(osdc)
add_subdirectory(pybind)
add_subdirectory(rgw)
add_subdirectory(rbd_mirror)
add_subdirectory(system)

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

# ceph_multi_stress_watch
add_executable(ceph_multi_stress_watch
  multi_stress_watch.cc
  )
target_link_libraries(ceph_multi_stress_watch librados global radostest
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

#ceph_perf_local
add_executable(ceph_perf_local 
  perf_local.cc
  perf_helper.cc)
#INTEL_SSE & INTEL_SSE2 flags
set(PERF_LOCAL_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse2") 
set_target_properties(ceph_perf_local PROPERTIES COMPILE_FLAGS
  ${PERF_LOCAL_FLAGS})
target_link_libraries(ceph_perf_local os global ${UNITTEST_LIBS})

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

# ceph_test_keys
add_executable(ceph_test_keys
  testkeys.cc
  )
target_link_libraries(ceph_test_keys mon global ${CMAKE_DL_LIBS})

# ceph_test_snap_mapper
add_executable(ceph_test_snap_mapper EXCLUDE_FROM_ALL
  test_snap_mapper.cc
  )
target_link_libraries(ceph_test_snap_mapper osd global ${BLKID_LIBRARIES})

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

# ceph_test_librgw_file (nfs-like RGW interface)
add_executable(ceph_test_librgw_file
  librgw_file.cc
  )
set_target_properties(ceph_test_librgw_file PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_librgw_file
  rgw
  librados
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_librgw_file_cd (just the rgw_file create-delete bucket ops)
add_executable(ceph_test_librgw_file_cd
  librgw_file_cd.cc
  )
set_target_properties(ceph_test_librgw_file_cd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_librgw_file_cd
  rgw
  librados
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_librgw_file_gp (just the rgw_file get-put bucket ops)
add_executable(ceph_test_librgw_file_gp
  librgw_file_gp.cc
  )
set_target_properties(ceph_test_librgw_file_gp PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_librgw_file_gp
  rgw
  librados
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_librgw_file_nfsns (nfs namespace tests)
add_executable(ceph_test_librgw_file_nfsns
  librgw_file_nfsns.cc
  )
set_target_properties(ceph_test_librgw_file_nfsns PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_librgw_file_nfsns
  rgw
  librados
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_librgw_file_aw (nfs write transaction [atomic write] tests)
add_executable(ceph_test_librgw_file_aw
  librgw_file_aw.cc
  )
set_target_properties(ceph_test_librgw_file_aw PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_librgw_file_aw
  rgw
  librados
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_rgw_token
add_executable(ceph_test_rgw_token
  test_rgw_token.cc
  )
set_target_properties(ceph_test_rgw_token PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rgw_token
  rgw
  os
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# librgw_file_gp (just the rgw_file get-put bucket ops)
add_executable(test_rgw_ldap
  ${CMAKE_SOURCE_DIR}/src/rgw/rgw_ldap.cc
  test_rgw_ldap.cc
  )
set_target_properties(test_rgw_ldap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rgw_ldap
  librados
  ${OPENLDAP_LIBS}
  ${Boost_LIBRARIES}
  ${UNITTEST_LIBS}
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
  unittest_librbd
  ceph-objectstore-tool
  ceph-monstore-tool
  osdmaptool
  rbd
  radosgw-admin
  ceph_example
  ceph_snappy
  cls_lock
  ceph_test_objectstore
  ceph_erasure_code_non_regression
  ceph_erasure_code
  ceph-disk
  ceph-detect-init
  cython_modules)

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
add_dependencies(check run-tox-ceph-detect-init)

set_property(TEST 
  run-tox-ceph-disk
  run-tox-ceph-detect-init
  PROPERTY ENVIRONMENT 
  CEPH_ROOT=${CMAKE_SOURCE_DIR}
  CEPH_BIN=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
  CEPH_LIB=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
  LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/lib
  PATH=$ENV{PATH}:${CMAKE_RUNTIME_OUTPUT_DIRECTORY}:${CMAKE_SOURCE_DIR}/src
  PYTHONPATH=${CMAKE_SOURCE_DIR}/src/pybind
  )

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

#make check ends here

# unittest_direct_messenger
add_executable(unittest_direct_messenger EXCLUDE_FROM_ALL
                messenger/test_direct_messenger.cc)
add_test(unittest_direct_messenger unittest_direct_messenger)
add_dependencies(check unittest_direct_messenger)
target_link_libraries(unittest_direct_messenger global boost_system)
set_target_properties(unittest_direct_messenger
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS}
)

add_executable(time_direct EXCLUDE_FROM_ALL
                messenger/time_direct.cc)
add_test(time_direct time_direct)
add_dependencies(check time_direct)
target_link_libraries(time_direct global boost_system
  ${UNITTEST_LIBS}
  ${EXTRALIBS})
set_target_properties(time_direct
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS}
)
