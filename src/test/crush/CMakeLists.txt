# unittest_crush_wrapper
add_executable(unittest_crush_wrapper
  CrushWrapper.cc)
add_ceph_unittest(unittest_crush_wrapper)
target_link_libraries(unittest_crush_wrapper global ${BLKID_LIBRARIES})

# unittest_crush
add_executable(unittest_crush
  crush.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_crush)
target_link_libraries(unittest_crush global m ${BLKID_LIBRARIES})

add_ceph_test(crush_weights.sh ${CMAKE_CURRENT_SOURCE_DIR}/crush_weights.sh)
