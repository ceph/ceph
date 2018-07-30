# radostest
add_library(libradostest_obj OBJECT test.cc)
set(libradostest_srcs 
  test_common.cc
  TestCase.cc)
add_library(radostest STATIC
  ${libradostest_srcs}
  $<TARGET_OBJECTS:libradostest_obj>)
target_link_libraries(radostest librados ceph-common json_spirit ${EXTRALIBS})
set_target_properties(radostest PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# ceph_test_rados_api_cmd
add_executable(ceph_test_rados_api_cmd
  cmd.cc
  )
set_target_properties(ceph_test_rados_api_cmd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_cmd
  librados ${UNITTEST_LIBS} radostest)

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

# ceph_test_rados_api_asio
add_executable(ceph_test_rados_api_asio asio.cc)
set_target_properties(ceph_test_rados_api_asio PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_asio global
  librados ${UNITTEST_LIBS})
if(WITH_BOOST_CONTEXT)
  target_link_libraries(ceph_test_rados_api_asio Boost::coroutine Boost::context)
endif()

# ceph_test_rados_api_list
add_executable(ceph_test_rados_api_list
  list.cc
  $<TARGET_OBJECTS:unit-main>
  )
set_target_properties(ceph_test_rados_api_list PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_list
  librados global ${UNITTEST_LIBS} radostest)

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
  $<TARGET_OBJECTS:unit-main>
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

# ceph_test_rados_api_service
add_executable(ceph_test_rados_api_service
  service.cc
  )
set_target_properties(ceph_test_rados_api_service PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_service
  librados global ${UNITTEST_LIBS} radostest)

# ceph_test_rados_api_tier
add_executable(ceph_test_rados_api_tier
  tier.cc
  $<TARGET_OBJECTS:unit-main>
  )
set_target_properties(ceph_test_rados_api_tier PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_tier
  librados global ${UNITTEST_LIBS} Boost::system radostest)

# ceph_test_rados_api_snapshots
add_executable(ceph_test_rados_api_snapshots
  snapshots.cc
  )
set_target_properties(ceph_test_rados_api_snapshots PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_rados_api_snapshots
  librados ${UNITTEST_LIBS} radostest)

install(TARGETS
  ceph_test_rados_api_aio
  ceph_test_rados_api_asio
  ceph_test_rados_api_c_read_operations
  ceph_test_rados_api_c_write_operations
  ceph_test_rados_api_cmd
  ceph_test_rados_api_io
  ceph_test_rados_api_list
  ceph_test_rados_api_lock
  ceph_test_rados_api_misc
  ceph_test_rados_api_pool
  ceph_test_rados_api_service
  ceph_test_rados_api_snapshots
  ceph_test_rados_api_stat
  ceph_test_rados_api_tier
  ceph_test_rados_api_watch_notify
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# unittest_librados
add_executable(unittest_librados
  librados.cc
  )
add_ceph_unittest(unittest_librados)
target_link_libraries(unittest_librados librados ${BLKID_LIBRARIES})

# unittest_librados_config
add_executable(unittest_librados_config
  librados_config.cc
  )
add_ceph_unittest(unittest_librados_config)
target_link_libraries(unittest_librados_config
  librados
  ${BLKID_LIBRARIES}
  )

