add_executable(ceph_test_filepath
  test.cc
  $<TARGET_OBJECTS:unit-main>
  )
target_link_libraries(ceph_test_filepath os global ceph-common ${CMAKE_DL_LIBS} ${UNITTEST_LIBS})

install(TARGETS
  ceph_test_filepath
  DESTINATION ${CMAKE_INSTALL_BINDIR})
