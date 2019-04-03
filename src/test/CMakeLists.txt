include(AddCephTest)

set(UNITTEST_LIBS GMock::Main GMock::GMock GTest::GTest ${CMAKE_THREAD_LIBS_INIT} 
    ${GSSAPI_LIBRARIES} ${OPENLDAP_LIBRARIES} ${CMAKE_DL_LIBS})

add_library(unit-main OBJECT unit.cc)
target_include_directories(unit-main PRIVATE
  $<TARGET_PROPERTY:GTest::GTest,INTERFACE_INCLUDE_DIRECTORIES>)

add_subdirectory(cls_hello)
add_subdirectory(cls_lock)
add_subdirectory(cls_log)
add_subdirectory(cls_numops)
add_subdirectory(cls_sdk)
if(WITH_RBD)
  add_subdirectory(cls_journal)
  add_subdirectory(cls_rbd)
endif(WITH_RBD)
add_subdirectory(cls_refcount)
add_subdirectory(cls_rgw)
add_subdirectory(cls_version)
add_subdirectory(cls_lua)
add_subdirectory(common)
add_subdirectory(compressor)
add_subdirectory(crush)
add_subdirectory(direct_messenger)
add_subdirectory(encoding)
add_subdirectory(erasure-code)
add_subdirectory(filestore)
add_subdirectory(fs)
add_subdirectory(journal)
add_subdirectory(libcephfs)
add_subdirectory(librados)
add_subdirectory(librados_test_stub)
if(WITH_LIBRADOSSTRIPER)
  add_subdirectory(libradosstriper)
endif()
if(WITH_RBD)
  add_subdirectory(librbd)
endif(WITH_RBD)
if (WITH_CEPHFS)
  add_subdirectory(mds)
endif()
add_subdirectory(mon)
add_subdirectory(mgr)
add_subdirectory(msgr)
add_subdirectory(ObjectMap)
add_subdirectory(objectstore)
add_subdirectory(os)
add_subdirectory(osd)
add_subdirectory(osdc)
add_subdirectory(pybind)
add_subdirectory(immutable_object_cache)
if(WITH_RADOSGW)
  set(rgw_libs rgw_a)
  if(WITH_RADOSGW_AMQP_ENDPOINT)
    list(APPEND rgw_libs amqp_mock)
  endif()
  add_subdirectory(rgw)
endif(WITH_RADOSGW)
if(WITH_RBD)
  add_subdirectory(rbd_mirror)
endif(WITH_RBD)
if(WITH_SEASTAR)
  add_subdirectory(crimson)
endif()
add_subdirectory(system)
if(WITH_FIO OR WITH_SYSTEM_FIO)
  add_subdirectory(fio)
endif()

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
target_link_libraries(ceph_test_rewrite_latency ceph-common
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
target_link_libraries(test_build_libcommon ceph-common pthread ${CRYPTO_LIBS} ${EXTRALIBS})

if(WITH_RADOSGW)
  add_executable(test_build_librgw buildtest_skeleton.cc)
  target_link_libraries(test_build_librgw rgw_a pthread ${CRYPTO_LIBS} ${EXTRALIBS})
endif(WITH_RADOSGW)

if(WITH_RADOSGW)
  add_executable(test_build_librgw_admin_user buildtest_skeleton.cc)
  target_link_libraries(test_build_librgw_admin_user pthread ${CRYPTO_LIBS} ${EXTRALIBS})
endif(WITH_RADOSGW)

if(WITH_LIBCEPHFS)
  # From src/test/Makefile-client.am: I dont get this one... testing the osdc build but link in libcephfs?
  add_executable(test_build_libcephfs buildtest_skeleton.cc)
  target_link_libraries(test_build_libcephfs cephfs pthread ${CRYPTO_LIBS} ${EXTRALIBS})
endif(WITH_LIBCEPHFS)

add_executable(test_build_librados buildtest_skeleton.cc)
target_link_libraries(test_build_librados librados pthread ${CRYPTO_LIBS} ${EXTRALIBS} osdc osd os ceph-common cls_lock_client ${BLKID_LIBRARIES})

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
  Boost::program_options
  global
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

if(WITH_KVS)
  # ceph_kvstorebench
  set(kvstorebench_srcs
    kv_store_bench.cc
    ${CMAKE_SOURCE_DIR}/src/key_value_store/kv_flat_btree_async.cc
    )
  add_executable(ceph_kvstorebench ${kvstorebench_srcs})
  target_link_libraries(ceph_kvstorebench librados global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})
  install(TARGETS ceph_kvstorebench DESTINATION bin)
endif(WITH_KVS)

# ceph_objectstore_bench
add_executable(ceph_objectstore_bench objectstore_bench.cc)
target_link_libraries(ceph_objectstore_bench os global ${BLKID_LIBRARIES})

if(${WITH_RADOSGW})
  # test_cors
  set(test_cors_srcs test_cors.cc)
  add_executable(test_cors
    ${test_cors_srcs}
    )
  target_link_libraries(test_cors
    librados
    ${rgw_libs}
    global
    ${BLKID_LIBRARIES}
    ${CURL_LIBRARIES}
    ${EXPAT_LIBRARIES}
    ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

  # ceph_test_cls_rgw_meta
  set(test_cls_rgw_meta_srcs test_rgw_admin_meta.cc)
  add_executable(ceph_test_cls_rgw_meta
    ${test_cls_rgw_meta_srcs}
    )
  target_link_libraries(ceph_test_cls_rgw_meta
    librados
    ${rgw_libs}
    global
    cls_version_client
    cls_log_client
    cls_refcount_client
    cls_rgw_client
    cls_user_client
    cls_lock_client
    ${BLKID_LIBRARIES}
    ${CURL_LIBRARIES}
    ${EXPAT_LIBRARIES}
    ${CMAKE_DL_LIBS} ${UNITTEST_LIBS} ${CRYPTO_LIBS})

  install(TARGETS
    ceph_test_cls_rgw_meta
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  # ceph_test_cls_rgw_log
  set(ceph_test_cls_rgw_log_srcs
    test_rgw_admin_log.cc
    )
  add_executable(ceph_test_cls_rgw_log
    ${ceph_test_cls_rgw_log_srcs}
    )
  target_link_libraries(ceph_test_cls_rgw_log
    librados
    ${rgw_libs}
    global
    cls_version_client
    cls_log_client
    cls_refcount_client
    cls_rgw_client
    cls_user_client
    cls_lock_client
    ${BLKID_LIBRARIES}
    ${CURL_LIBRARIES}
    ${EXPAT_LIBRARIES}
    ${CMAKE_DL_LIBS}
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CRYPTO_LIBS}
    )

# ceph_test_librgw_file (nfs-like RGW interface)
add_executable(ceph_test_librgw_file
  librgw_file.cc
  )
if(WITH_RADOSGW_FCGI_FRONTEND)
target_include_directories(ceph_test_librgw_file SYSTEM PRIVATE ${FCGI_INCLUDE_DIR})
endif(WITH_RADOSGW_FCGI_FRONTEND)
target_link_libraries(ceph_test_librgw_file
  rgw
  librados
  ceph-common
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_librgw_file_cd (just the rgw_file create-delete bucket ops)
add_executable(ceph_test_librgw_file_cd
  librgw_file_cd.cc
  )
target_link_libraries(ceph_test_librgw_file_cd
  rgw
  librados
  ceph-common
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_librgw_file_gp (just the rgw_file get-put bucket ops)
add_executable(ceph_test_librgw_file_gp
  librgw_file_gp.cc
  )
target_link_libraries(ceph_test_librgw_file_gp
  rgw
  librados
  ceph-common
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_librgw_file_nfsns (nfs namespace tests)
add_executable(ceph_test_librgw_file_nfsns
  librgw_file_nfsns.cc
  )
if(WITH_RADOSGW_FCGI_FRONTEND)
target_include_directories(ceph_test_librgw_file_nfsns SYSTEM PRIVATE ${FCGI_INCLUDE_DIR})
endif(WITH_RADOSGW_FCGI_FRONTEND)
target_link_libraries(ceph_test_librgw_file_nfsns
  rgw
  librados
  ceph-common
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_librgw_file_aw (nfs write transaction [atomic write] tests)
add_executable(ceph_test_librgw_file_aw
  librgw_file_aw.cc
  )
target_link_libraries(ceph_test_librgw_file_aw
  rgw
  librados
  ceph-common
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_librgw_file_marker (READDIR with string and uint64 offsets)
add_executable(ceph_test_librgw_file_marker
  librgw_file_marker.cc
  )
target_link_libraries(ceph_test_librgw_file_marker
  rgw
  librados
  ceph-common
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  )

# ceph_test_rgw_token
add_executable(ceph_test_rgw_token
  test_rgw_token.cc
  )
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
target_link_libraries(test_rgw_ldap
  librados
  ceph-common
  ${OPENLDAP_LIBRARIES}
  ${UNITTEST_LIBS}
  )

# unittest_librgw_admin_user
add_executable(ceph_test_librgw_admin_user test_librgw_admin_user.cc)
target_link_libraries(ceph_test_librgw_admin_user
  rgw_admin_user
  cls_otp_client
  librados
  ceph-common
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
   )

endif(${WITH_RADOSGW})

# ceph_multi_stress_watch
add_executable(ceph_multi_stress_watch
  multi_stress_watch.cc
  )
target_link_libraries(ceph_multi_stress_watch librados global radostest-cxx
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

#ceph_perf_local
add_executable(ceph_perf_local
  perf_local.cc
  perf_helper.cc)
if(HAVE_SSE)
  set(PERF_LOCAL_FLAGS ${SSE3_FLAGS})
endif(HAVE_SSE)
if(HAVE_NEON)
  set(PERF_LOCAL_FLAGS ${ARM_NEON_FLAGS})
endif(HAVE_NEON)
if(PERF_LOCAL_FLAGS)
  set_target_properties(ceph_perf_local PROPERTIES COMPILE_FLAGS
    ${PERF_LOCAL_FLAGS})
endif()
target_link_libraries(ceph_perf_local os global ${UNITTEST_LIBS})

install(TARGETS
  ceph_bench_log
  ceph_multi_stress_watch
  ceph_objectstore_bench
  ceph_omapbench
  ceph_perf_local
  DESTINATION bin)

# ceph_test_filejournal
add_executable(ceph_test_filejournal
  test_filejournal.cc
  )
target_link_libraries(ceph_test_filejournal
  os
  ceph-common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  )
install(TARGETS
  ceph_test_filejournal
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# ceph_test_keys
add_executable(ceph_test_keys
  testkeys.cc
  )
target_link_libraries(ceph_test_keys mon global ${CMAKE_DL_LIBS})

# ceph_test_snap_mapper
add_executable(ceph_test_snap_mapper
  test_snap_mapper.cc
  $<TARGET_OBJECTS:unit-main>
  )
target_link_libraries(ceph_test_snap_mapper osd global ${BLKID_LIBRARIES} ${UNITTEST_LIBS})

add_executable(ceph_test_stress_watch
  test_stress_watch.cc
  )
target_link_libraries(ceph_test_stress_watch
  librados
  ${UNITTEST_LIBS}
  radostest-cxx
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS
  ceph_test_stress_watch
  DESTINATION ${CMAKE_INSTALL_BINDIR})

if(WITH_FUSE)
  add_executable(ceph_test_cfuse_cache_invalidate
    test_cfuse_cache_invalidate.cc
    )
  target_link_libraries(ceph_test_cfuse_cache_invalidate
    ceph-common
  )
endif(WITH_FUSE)

if(${WITH_CEPHFS})
  add_executable(test_c_headers
    test_c_headers.c
    )
  target_link_libraries(test_c_headers
    librados
    cephfs)
endif(${WITH_CEPHFS})

if(HAVE_BLKID OR FREEBSD)
  add_executable(ceph_test_get_blkdev_props
    test_get_blkdev_props.cc
    )
  target_link_libraries(ceph_test_get_blkdev_props
    ceph-common
    pthread
    ${EXTRALIBS}
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    )
endif(HAVE_BLKID OR FREEBSD)

# ceph_test_admin_socket_output

if(StdFilesystem_FOUND)
  add_executable(ceph_test_admin_socket_output
    test_admin_socket_output.cc
    admin_socket_output.cc
    admin_socket_output_tests.cc)
  target_link_libraries(ceph_test_admin_socket_output
    ceph-common StdFilesystem::filesystem)
  install(TARGETS
    ceph_test_admin_socket_output
    DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()

#make check starts here

#following dependencies are run inside make check unit tests
add_dependencies(tests
  ceph-mon
  ceph-authtool
  get_command_descriptions
  crushtool
  ceph-conf
  rados
  monmaptool
  ceph-osd
  ceph-dencoder
  ceph-objectstore-tool
  ceph-kvstore-tool
  ceph-monstore-tool
  osdmaptool
  ceph_example
  ceph_snappy
  cls_lock
  ceph_test_objectstore
  ceph_erasure_code_non_regression
  ceph_erasure_code
  cython${PY_BINDING_INFIX}_modules)
if (WITH_CEPHFS)
  add_dependencies(tests ceph-mds)
endif()
if(WITH_MGR)
  add_dependencies(tests ceph-mgr)
endif()
if(WITH_RBD)
  add_dependencies(tests unittest_librbd rbd)
  if(FREEBSD)
    add_dependencies(tests rbd-ggate)
  endif(FREEBSD)
endif(WITH_RBD)
if(WITH_RADOSGW)
  add_dependencies(tests radosgw radosgw-admin)
endif(WITH_RADOSGW)
#add dependency from fio just to ensure the plugin build isn't failing
if(WITH_FIO OR WITH_SYSTEM_FIO)
  add_dependencies(tests fio_ceph_objectstore)
endif()

if(WITH_RBD)
  add_ceph_test(run-rbd-unit-tests.sh ${CMAKE_CURRENT_SOURCE_DIR}/run-rbd-unit-tests.sh)
  if(FREEBSD)
    add_ceph_test(rbd-ggate.sh ${CMAKE_CURRENT_SOURCE_DIR}/rbd-ggate.sh)
  endif(FREEBSD)
endif(WITH_RBD)
add_ceph_test(run-cli-tests ${CMAKE_CURRENT_SOURCE_DIR}/run-cli-tests)
add_ceph_test(test_objectstore_memstore.sh ${CMAKE_CURRENT_SOURCE_DIR}/test_objectstore_memstore.sh)

# flaky
#add_ceph_test(test_pidfile.sh ${CMAKE_CURRENT_SOURCE_DIR}/test_pidfile.sh)

add_ceph_test(smoke.sh ${CMAKE_CURRENT_SOURCE_DIR}/smoke.sh)

# XXX are these safe to remove? they used to be around for ceph-disk tox testing
set(env_vars_for_tox_tests
  CEPH_BUILD_DIR=${CMAKE_BINARY_DIR}
  CEPH_ROOT=${CMAKE_SOURCE_DIR}
  CEPH_BIN=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
  CEPH_LIB=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
  CEPH_BUILD_VIRTUALENV=${CEPH_BUILD_VIRTUALENV}
  LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/lib
  PATH=$ENV{PATH}:${CMAKE_RUNTIME_OUTPUT_DIRECTORY}:${CMAKE_SOURCE_DIR}/src
  PYTHONPATH=${CMAKE_SOURCE_DIR}/src/pybind
  WITH_PYTHON2=${WITH_PYTHON2}
  WITH_PYTHON3=${WITH_PYTHON3})

if(WITH_MGR)
  add_test(NAME run-tox-mgr-dashboard COMMAND bash ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/run-tox.sh)
  list(APPEND tox_tests run-tox-mgr-dashboard)
  set(MGR_DASHBOARD_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/mgr-dashboard-virtualenv)
  list(APPEND env_vars_for_tox_tests MGR_DASHBOARD_VIRTUALENV=${MGR_DASHBOARD_VIRTUALENV})

  add_test(NAME run-tox-mgr-insights COMMAND bash ${CMAKE_SOURCE_DIR}/src/pybind/mgr/insights/run-tox.sh)
  list(APPEND tox_tests run-tox-mgr-insights)
  set(MGR_INSIGHTS_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/mgr-insights-virtualenv)
  list(APPEND env_vars_for_tox_tests MGR_INSIGHTS_VIRTUALENV=${MGR_INSIGHTS_VIRTUALENV})

  add_test(NAME run-tox-mgr-ansible COMMAND bash ${CMAKE_SOURCE_DIR}/src/pybind/mgr/ansible/run-tox.sh)
  list(APPEND tox_tests run-tox-mgr-ansible)
  set(MGR_ANSIBLE_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/mgr-ansible-virtualenv)
  list(APPEND env_vars_for_tox_tests MGR_ANSIBLE_VIRTUALENV=${MGR_ANSIBLE_VIRTUALENV})

  add_test(NAME run-tox-mgr-orchestrator_cli COMMAND bash ${CMAKE_SOURCE_DIR}/src/pybind/mgr/orchestrator_cli/run-tox.sh)
  list(APPEND tox_tests run-tox-mgr-orchestrator_cli)
  set(MGR_ORCHESTRATOR_CLI_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/mgr-orchestrator_cli-virtualenv)
  list(APPEND env_vars_for_tox_tests MGR_ORCHESTRATOR_CLI_VIRTUALENV=${MGR_ORCHESTRATOR_CLI_VIRTUALENV})
endif()

set_property(
  TEST ${tox_tests}
  PROPERTY ENVIRONMENT ${env_vars_for_tox_tests})

# unittest_admin_socket
add_executable(unittest_admin_socket
  admin_socket.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_admin_socket)
target_link_libraries(unittest_admin_socket global)

# unittest_encoding
add_executable(unittest_encoding
  encoding.cc
  )
add_ceph_unittest(unittest_encoding)
target_link_libraries(unittest_encoding ceph-common)

# unittest_addrs
add_executable(unittest_addrs
  test_addrs.cc
  )
add_ceph_unittest(unittest_addrs)
target_link_libraries(unittest_addrs ceph-common)

# unittest_auth
add_executable(unittest_auth
  test_auth.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_auth)
target_link_libraries(unittest_auth global)

# unittest_workqueue
add_executable(unittest_workqueue
  test_workqueue.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_workqueue)
target_link_libraries(unittest_workqueue global)

# unittest_striper
if(WITH_LIBRADOSSTRIPER)
  add_executable(unittest_striper
    test_striper.cc
    $<TARGET_OBJECTS:unit-main>
    )
  add_ceph_unittest(unittest_striper)
  target_link_libraries(unittest_striper global ${BLKID_LIBRARIES})
endif()

# unittest_str_list
add_executable(unittest_str_list
  test_str_list.cc
  )
add_ceph_unittest(unittest_str_list)
target_link_libraries(unittest_str_list global)

# unittest_log
add_executable(unittest_log
  ${CMAKE_SOURCE_DIR}/src/log/test.cc
  )
add_ceph_unittest(unittest_log)
target_link_libraries(unittest_log global)

# unittest_base64
add_executable(unittest_base64
  base64.cc
  )
add_ceph_unittest(unittest_base64)
target_link_libraries(unittest_base64 global)

# unittest_ceph_argparse
add_executable(unittest_ceph_argparse
  ceph_argparse.cc
  )
add_ceph_unittest(unittest_ceph_argparse)
target_link_libraries(unittest_ceph_argparse global)

# unittest_ceph_compatset
add_executable(unittest_ceph_compatset
  ceph_compatset.cc
  )
add_ceph_unittest(unittest_ceph_compatset)
target_link_libraries(unittest_ceph_compatset global)

# unittest_gather
add_executable(unittest_gather
  gather.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_gather)
target_link_libraries(unittest_gather global)

# unittest_run_cmd
add_executable(unittest_run_cmd
  run_cmd.cc
  )
add_ceph_unittest(unittest_run_cmd)
target_link_libraries(unittest_run_cmd global)

# signals
add_executable(unittest_signals
  signals.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_signals)
target_link_libraries(unittest_signals global)

# unittest_simple_spin
add_executable(unittest_simple_spin
  simple_spin.cc
  )
add_ceph_unittest(unittest_simple_spin)
target_link_libraries(unittest_simple_spin global)

# unittest_bufferlist
add_executable(unittest_bufferlist
  bufferlist.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_bufferlist)
target_link_libraries(unittest_bufferlist global)

# unittest_xlist
add_executable(unittest_xlist
  test_xlist.cc
  )
add_ceph_unittest(unittest_xlist)
target_link_libraries(unittest_xlist ceph-common)

# unittest_arch
add_executable(unittest_arch
  test_arch.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_arch)
target_link_libraries(unittest_arch global)

# unittest_denc
add_executable(unittest_denc
  test_denc.cc
  )
add_ceph_unittest(unittest_denc)
target_link_libraries(unittest_denc global)

# unittest_mempool
add_executable(unittest_mempool
  test_mempool.cc
  )
add_ceph_unittest(unittest_mempool)
target_link_libraries(unittest_mempool global)

# unittest_features
add_executable(unittest_features
  test_features.cc
  )
add_ceph_unittest(unittest_features)
target_link_libraries(unittest_features global)

# unittest_crypto
add_executable(unittest_crypto
  crypto.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_crypto)
target_link_libraries(unittest_crypto global)

# unittest_crypto_init
add_executable(unittest_crypto_init
  crypto_init.cc
  )
add_ceph_unittest(unittest_crypto_init)
target_link_libraries(unittest_crypto_init global)

# unittest_perf_counters
add_executable(unittest_perf_counters
  perf_counters.cc
  )
add_ceph_unittest(unittest_perf_counters)
target_link_libraries(unittest_perf_counters global)

# unittest_ceph_crypto
add_executable(unittest_ceph_crypto
  ceph_crypto.cc
  )
add_ceph_unittest(unittest_ceph_crypto)
target_link_libraries(unittest_ceph_crypto global)

# unittest_utf8
add_executable(unittest_utf8
  utf8.cc
  )
add_ceph_unittest(unittest_utf8)
target_link_libraries(unittest_utf8 global)

# unittest_mime
add_executable(unittest_mime
  mime.cc
  )
add_ceph_unittest(unittest_mime)
target_link_libraries(unittest_mime global)

# unittest_escape
add_executable(unittest_escape
  escape.cc
  )
add_ceph_unittest(unittest_escape)
target_link_libraries(unittest_escape global)

# unittest_strtol
add_executable(unittest_strtol
  strtol.cc
  )
add_ceph_unittest(unittest_strtol)
target_link_libraries(unittest_strtol global)

# unittest_confutils
add_executable(unittest_confutils
  confutils.cc
  )
add_ceph_unittest(unittest_confutils)
target_link_libraries(unittest_confutils global)

# unittest_heartbeatmap
add_executable(unittest_heartbeatmap
  heartbeat_map.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_heartbeatmap)
target_link_libraries(unittest_heartbeatmap global ceph-common)

if(${WITH_RADOSGW})
  # unittest_formatter
  add_executable(unittest_formatter
    formatter.cc
    $<TARGET_OBJECTS:unit-main>
    )
  add_ceph_unittest(unittest_formatter)
  target_link_libraries(unittest_formatter global)
endif(${WITH_RADOSGW})

# unittest_daemon_config
add_executable(unittest_daemon_config
  daemon_config.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_daemon_config)
target_link_libraries(unittest_daemon_config
  ceph-common
  global
  ${BLKID_LIBRARIES}
  ${EXTRALIBS}
  )

if(WITH_LIBCEPHFS)
# unittest_libcephfs_config
add_executable(unittest_libcephfs_config
  libcephfs_config.cc
  )
add_ceph_unittest(unittest_libcephfs_config)
target_link_libraries(unittest_libcephfs_config cephfs)
endif(WITH_LIBCEPHFS)

if(WITH_RBD)
# unittest_rbd_replay
add_executable(unittest_rbd_replay
  test_rbd_replay.cc)
add_ceph_unittest(unittest_rbd_replay)
target_link_libraries(unittest_rbd_replay
  librbd
  librados
  global
  rbd_replay
  rbd_replay_ios
  ${BLKID_LIBRARIES}
  )
endif(WITH_RBD)

# unittest_ipaddr
add_executable(unittest_ipaddr
  test_ipaddr.cc)
add_ceph_unittest(unittest_ipaddr)
target_link_libraries(unittest_ipaddr mon global)

# unittest_texttable
add_executable(unittest_texttable
  test_texttable.cc
  $<TARGET_OBJECTS:common_texttable_obj>
  )
add_ceph_unittest(unittest_texttable)
target_link_libraries(unittest_texttable mon global)

# unittest_on_exit
add_executable(unittest_on_exit
  on_exit.cc
  )
add_ceph_unittest(unittest_on_exit)
target_link_libraries(unittest_on_exit global)

# unittest_subprocess
add_executable(unittest_subprocess
  test_subprocess.cc
  )
add_ceph_unittest(unittest_subprocess)
target_link_libraries(unittest_subprocess global)

# unittest_pageset
add_executable(unittest_pageset test_pageset.cc)
add_ceph_unittest(unittest_pageset)
target_link_libraries(unittest_pageset global)

# unittest_any_
add_executable(unittest_any test_any.cc)
add_ceph_unittest(unittest_any)
target_link_libraries(unittest_any)

# unittest_weighted_shuffle
add_executable(unittest_weighted_shuffle test_weighted_shuffle.cc)
add_ceph_unittest(unittest_weighted_shuffle)

#make check ends here
