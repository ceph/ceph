add_executable(unittest_seastar_buffer
  test_buffer.cc)
add_ceph_unittest(unittest_seastar_buffer)
target_link_libraries(unittest_seastar_buffer ceph-common crimson)

add_executable(unittest_seastar_denc
  test_denc.cc
  $<TARGET_OBJECTS:unit-main>)
add_ceph_unittest(unittest_seastar_denc)
target_link_libraries(unittest_seastar_denc ceph-common global crimson)

add_executable(unittest_seastar_messenger test_messenger.cc)
target_link_libraries(unittest_seastar_messenger ceph-common crimson)

add_executable(unittest_seastar_echo
  test_alien_echo.cc)
target_link_libraries(unittest_seastar_echo ceph-common global crimson)

add_executable(unittest_seastar_thread_pool
  test_thread_pool.cc)
add_ceph_unittest(unittest_seastar_thread_pool)
target_link_libraries(unittest_seastar_thread_pool crimson)
