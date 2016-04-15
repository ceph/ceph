# ceph_test_async_driver
add_executable(ceph_test_async_driver
  test_async_driver.cc
  )
set_target_properties(ceph_test_async_driver PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_async_driver os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

# ceph_test_msgr
add_executable(ceph_test_msgr
  test_msgr.cc
  )
set_target_properties(ceph_test_msgr PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_msgr os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

#ceph_perf_msgr_server
add_executable(ceph_perf_msgr_server perf_msgr_server.cc)
set_target_properties(ceph_perf_msgr_server PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_perf_msgr_server os global ${UNITTEST_LIBS})

#ceph_perf_msgr_client
add_executable(ceph_perf_msgr_client perf_msgr_client.cc)
set_target_properties(ceph_perf_msgr_client PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_perf_msgr_client os global ${UNITTEST_LIBS})

