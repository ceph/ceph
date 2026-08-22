# ceph_test_async_driver
add_executable(ceph_test_async_driver
  test_async_driver.cc
  $<TARGET_OBJECTS:unit-main>
  )
target_link_libraries(ceph_test_async_driver os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

# ceph_test_msgr
add_executable(ceph_test_msgr
  test_msgr.cc
  )
target_link_libraries(ceph_test_msgr os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

# ceph_test_async_networkstack
add_executable(ceph_test_async_networkstack
  test_async_networkstack.cc
  $<TARGET_OBJECTS:unit-main>
  )
target_link_libraries(ceph_test_async_networkstack global ${CRYPTO_LIBS} ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

#ceph_perf_msgr_server
add_executable(ceph_perf_msgr_server perf_msgr_server.cc)
target_link_libraries(ceph_perf_msgr_server os global ${UNITTEST_LIBS})

#ceph_perf_msgr_client
add_executable(ceph_perf_msgr_client perf_msgr_client.cc)
target_link_libraries(ceph_perf_msgr_client os global ${UNITTEST_LIBS})

# unitttest_frames_v2
add_executable(unittest_frames_v2 test_frames_v2.cc)
add_ceph_unittest(unittest_frames_v2)
target_link_libraries(unittest_frames_v2 os global ${UNITTEST_LIBS})

# unittest_psp_netlink_mock. The mock backend is built here rather
# than into libceph-common: it is a test double with failure-injection
# knobs and has no business in a shipped library.
if(HAVE_PSP)
  add_executable(unittest_psp_netlink_mock
    test_psp_netlink_mock.cc
    ${PROJECT_SOURCE_DIR}/src/msg/async/PSPNetlinkMock.cc)
  add_ceph_unittest(unittest_psp_netlink_mock)
  target_link_libraries(unittest_psp_netlink_mock global ${UNITTEST_LIBS})
endif()

add_executable(unittest_comp_registry
  test_comp_registry.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_comp_registry)
target_link_libraries(unittest_comp_registry global)

# test_userspace_event
if(HAVE_DPDK)
  add_executable(ceph_test_userspace_event
    test_userspace_event.cc
    $<TARGET_OBJECTS:unit-main>)
  target_link_libraries(ceph_test_userspace_event
    global
    ${CMAKE_DL_LIBS}
    ${UNITTEST_LIBS})
endif(HAVE_DPDK)

install(TARGETS
  ceph_test_async_driver
  ceph_test_msgr
  ceph_test_async_networkstack
  ceph_perf_msgr_server
  ceph_perf_msgr_client
  DESTINATION ${CMAKE_INSTALL_BINDIR})
