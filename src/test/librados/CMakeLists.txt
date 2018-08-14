# radostest
add_library(libradostest_obj OBJECT test.cc)
target_include_directories(libradostest_obj PRIVATE
  $<TARGET_PROPERTY:GTest::GTest,INTERFACE_INCLUDE_DIRECTORIES>)
set(libradostest_srcs 
  test_common.cc
  TestCase.cc)
add_library(radostest STATIC
  ${libradostest_srcs}
  $<TARGET_OBJECTS:libradostest_obj>)
target_link_libraries(radostest
  GTest::GTest
  librados
  ceph-common
  json_spirit
  ${EXTRALIBS})


add_executable(ceph_test_rados_api_cmd
  cmd.cc)
target_link_libraries(ceph_test_rados_api_cmd
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_io
  io.cc)
target_link_libraries(ceph_test_rados_api_io
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_c_write_operations
  c_write_operations.cc)
target_link_libraries(ceph_test_rados_api_c_write_operations
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_c_read_operations
  c_read_operations.cc)
target_link_libraries(ceph_test_rados_api_c_read_operations
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_aio
  aio.cc)
target_link_libraries(ceph_test_rados_api_aio
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_asio asio.cc)
target_link_libraries(ceph_test_rados_api_asio global
  librados ${UNITTEST_LIBS})
if(WITH_BOOST_CONTEXT)
  target_link_libraries(ceph_test_rados_api_asio Boost::coroutine Boost::context)
endif()

add_executable(ceph_test_rados_api_list
  list.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(ceph_test_rados_api_list
  librados global ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_pool
  pool.cc)
target_link_libraries(ceph_test_rados_api_pool
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_stat
  stat.cc)
target_link_libraries(ceph_test_rados_api_stat
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_watch_notify
  watch_notify.cc)
target_link_libraries(ceph_test_rados_api_watch_notify
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_cls
  cls.cc)
target_link_libraries(ceph_test_rados_api_cls
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_misc
  misc.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(ceph_test_rados_api_misc
  librados global ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_lock
  lock.cc)
target_link_libraries(ceph_test_rados_api_lock
  librados ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_service
  service.cc)
target_link_libraries(ceph_test_rados_api_service
  librados global ${UNITTEST_LIBS} radostest)

add_executable(ceph_test_rados_api_tier
  tier.cc
  $<TARGET_OBJECTS:unit-main>)
target_link_libraries(ceph_test_rados_api_tier
  librados global ${UNITTEST_LIBS} Boost::system radostest)

add_executable(ceph_test_rados_api_snapshots
  snapshots.cc)
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

