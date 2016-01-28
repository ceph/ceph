# radostest 
set(libradostest_srcs 
  test.cc 
  TestCase.cc)
add_library(radostest STATIC ${libradostest_srcs})
set_target_properties(radostest PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# ceph_test_rados_api_cmd
add_executable(ceph_test_rados_api_cmd
  cmd.cc
  )
set_target_properties(ceph_test_rados_api_cmd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_cmd
  librados global ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_io
add_executable(ceph_test_rados_api_io
  io.cc
  )
set_target_properties(ceph_test_rados_api_io PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_io
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_c_write_operations
add_executable(ceph_test_rados_api_c_write_operations
  c_write_operations.cc
  )
set_target_properties(ceph_test_rados_api_c_write_operations PROPERTIES
  COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_c_write_operations
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_c_read_operations
add_executable(ceph_test_rados_api_c_read_operations
  c_read_operations.cc
  )
set_target_properties(ceph_test_rados_api_c_read_operations PROPERTIES COMPILE_FLAGS 
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_c_read_operations
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_aio
add_executable(ceph_test_rados_api_aio
  aio.cc
  )
set_target_properties(ceph_test_rados_api_aio PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_aio
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_list
add_executable(ceph_test_rados_api_list
  list.cc
  )
set_target_properties(ceph_test_rados_api_list PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_list
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_nlist
add_executable(ceph_test_rados_api_nlist
  nlist.cc
  )
set_target_properties(ceph_test_rados_api_nlist PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_nlist
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_pool
add_executable(ceph_test_rados_api_pool
  pool.cc
  )
set_target_properties(ceph_test_rados_api_pool PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS}
  )
target_link_libraries(ceph_test_rados_api_pool
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_stat
add_executable(ceph_test_rados_api_stat
  stat.cc
  )
set_target_properties(ceph_test_rados_api_stat PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_stat
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_watch_notify
add_executable(ceph_test_rados_api_watch_notify
  watch_notify.cc
  )
set_target_properties(ceph_test_rados_api_watch_notify PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_watch_notify
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_cls
add_executable(ceph_test_rados_api_cls
  cls.cc
  )
set_target_properties(ceph_test_rados_api_cls PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_cls
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_misc
add_executable(ceph_test_rados_api_misc
  misc.cc
  )
set_target_properties(ceph_test_rados_api_misc PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_misc
  librados global ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_lock
add_executable(ceph_test_rados_api_lock
  lock.cc
  )
set_target_properties(ceph_test_rados_api_lock PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_lock
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_tier
add_executable(ceph_test_rados_api_tier
  tier.cc
  )
set_target_properties(ceph_test_rados_api_tier PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_tier
  global librados ${UNITTEST_LIBS} ${Boost_SYSTEM_LIBRARY} radostest)

# ceph_test_rados_api_snapshots
add_executable(ceph_test_rados_api_snapshots
  snapshots.cc
  )
set_target_properties(ceph_test_rados_api_snapshots PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_snapshots
  librados ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_tmap_migrate
add_executable(ceph_test_rados_api_tmap_migrate
  ${CMAKE_SOURCE_DIR}/src/tools/cephfs/DataScan.cc
  ${CMAKE_SOURCE_DIR}/src/tools/cephfs/MDSUtility.cc
  tmap_migrate.cc
  )
set_target_properties(ceph_test_rados_api_tmap_migrate PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_tmap_migrate
  librados mds osdc global cls_cephfs_client ${UNITTEST_LIBS} radostest)

# unittest_librados
add_executable(unittest_librados EXCLUDE_FROM_ALL
  librados.cc
  )
add_ceph_unittest(unittest_librados ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_librados)
target_link_libraries(unittest_librados librados global ${BLKID_LIBRARIES})

# unittest_librados_config
add_executable(unittest_librados_config EXCLUDE_FROM_ALL
  librados_config.cc
  )
add_ceph_unittest(unittest_librados_config ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_librados_config)
target_link_libraries(unittest_librados_config
  librados
  global
  ${BLKID_LIBRARIES}
  )

