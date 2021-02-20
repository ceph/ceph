# unittest_crush_wrapper
add_executable(unittest_crush_wrapper
  CrushWrapper.cc)
add_ceph_unittest(unittest_crush_wrapper)
target_link_libraries(unittest_crush_wrapper ceph-common)

# unittest_crush
add_executable(unittest_crush
  crush.cc)
add_ceph_unittest(unittest_crush parallel)
target_link_libraries(unittest_crush ceph-common)

add_ceph_test(crush_weights.sh ${CMAKE_CURRENT_SOURCE_DIR}/crush_weights.sh)
