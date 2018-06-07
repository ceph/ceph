set(test_buffer_srcs
  test_buffer.cc
  $<TARGET_OBJECTS:seastar_buffer_obj>)
add_executable(unittest_seastar_buffer ${test_buffer_srcs})
add_ceph_unittest(unittest_seastar_buffer)
target_link_libraries(unittest_seastar_buffer ceph-common Seastar::seastar)

add_executable(unittest_seastar_denc
  test_denc.cc
  $<TARGET_OBJECTS:seastar_buffer_obj>
  $<TARGET_OBJECTS:unit-main>)
add_ceph_unittest(unittest_seastar_denc)
target_link_libraries(unittest_seastar_denc ceph-common global Seastar::seastar)

set(test_messenger_srcs
  test_messenger.cc
  $<TARGET_OBJECTS:seastar_buffer_obj>
  $<TARGET_OBJECTS:crimson_net_objs>
  $<TARGET_OBJECTS:crimson_thread_objs>)
add_executable(unittest_seastar_messenger ${test_messenger_srcs})
add_ceph_unittest(unittest_seastar_messenger)
target_link_libraries(unittest_seastar_messenger ceph-common Seastar::seastar)

set(test_alien_echo_srcs
  test_alien_echo.cc
  $<TARGET_OBJECTS:seastar_buffer_obj>
  $<TARGET_OBJECTS:crimson_net_objs>
  $<TARGET_OBJECTS:crimson_thread_objs>)
add_executable(unittest_seastar_echo ${test_alien_echo_srcs})
add_ceph_unittest(unittest_seastar_echo)
target_link_libraries(unittest_seastar_echo ceph-common global Seastar::seastar)

add_executable(unittest_seastar_thread_pool
  test_thread_pool.cc
  $<TARGET_OBJECTS:crimson_thread_objs>)
add_ceph_unittest(unittest_seastar_thread_pool)
target_link_libraries(unittest_seastar_thread_pool Seastar::seastar)
