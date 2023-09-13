if(WITH_DOKAN)
  add_executable(ceph_test_dokan
    dokan.cc
  )
  target_link_libraries(ceph_test_dokan
    ceph-common
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_dokan
    DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(WITH_DOKAN)
