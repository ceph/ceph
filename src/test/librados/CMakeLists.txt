# radostest 
set(libradostest_srcs 
  test.cc 
  TestCase.cc)
add_library(radostest STATIC ${libradostest_srcs})
target_link_libraries(radostest common json_spirit ${EXTRALIBS})
set_target_properties(radostest PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# ceph_test_rados_api_cmd
add_executable(ceph_test_rados_api_cmd
  cmd.cc
  )
set_target_properties(ceph_test_rados_api_cmd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_cmd
  rados_a global ${UNITTEST_LIBS} radostest)

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
  rados_a global ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_nlist
add_executable(ceph_test_rados_api_nlist
  nlist.cc
  )
set_target_properties(ceph_test_rados_api_nlist PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_nlist
  rados_a ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_pool
add_executable(ceph_test_rados_api_pool
  pool.cc
  )
set_target_properties(ceph_test_rados_api_pool PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS}
  )
target_link_libraries(ceph_test_rados_api_pool
  rados_a ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_stat
add_executable(ceph_test_rados_api_stat
  stat.cc
  )
set_target_properties(ceph_test_rados_api_stat PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_stat
  rados_a ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_watch_notify
add_executable(ceph_test_rados_api_watch_notify
  watch_notify.cc
  )
set_target_properties(ceph_test_rados_api_watch_notify PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_watch_notify
  rados_a ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_cls
add_executable(ceph_test_rados_api_cls
  cls.cc
  )
set_target_properties(ceph_test_rados_api_cls PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_cls
  rados_a ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_misc
add_executable(ceph_test_rados_api_misc
  misc.cc
  )
set_target_properties(ceph_test_rados_api_misc PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_misc
  rados_a global ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_lock
add_executable(ceph_test_rados_api_lock
  lock.cc
  )
set_target_properties(ceph_test_rados_api_lock PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_lock
  rados_a ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_tier
add_executable(ceph_test_rados_api_tier
  tier.cc
  )
set_target_properties(ceph_test_rados_api_tier PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_tier
  global rados_a ${UNITTEST_LIBS} ${Boost_SYSTEM_LIBRARY} radostest)

# ceph_test_rados_api_snapshots
add_executable(ceph_test_rados_api_snapshots
  snapshots.cc
  )
set_target_properties(ceph_test_rados_api_snapshots PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_snapshots
  rados_a ${UNITTEST_LIBS} radostest)

install(TARGETS
  ceph_test_rados_api_aio
  ceph_test_rados_api_c_read_operations
  ceph_test_rados_api_c_write_operations
  ceph_test_rados_api_cmd
  ceph_test_rados_api_io
  ceph_test_rados_api_list
  ceph_test_rados_api_lock
  ceph_test_rados_api_misc
  ceph_test_rados_api_pool
  ceph_test_rados_api_snapshots
  ceph_test_rados_api_stat
  ceph_test_rados_api_tier
  ceph_test_rados_api_watch_notify
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# unittest_librados
add_executable(unittest_librados
  librados.cc
  )
add_ceph_unittest(unittest_librados ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_librados)
target_link_libraries(unittest_librados librados ${BLKID_LIBRARIES})

# unittest_librados_config
add_executable(unittest_librados_config
  librados_config.cc
  )
add_ceph_unittest(unittest_librados_config ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_librados_config)
target_link_libraries(unittest_librados_config
  librados
  ${BLKID_LIBRARIES}
  )

