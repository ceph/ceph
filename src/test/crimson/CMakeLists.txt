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
