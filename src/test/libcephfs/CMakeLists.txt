if(${WITH_CEPHFS})
  add_executable(ceph_test_libcephfs
    test.cc
    readdir_r_cb.cc
    caps.cc
    multiclient.cc
    flock.cc
    acl.cc
    access.cc
  )
  set_target_properties(ceph_test_libcephfs PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(ceph_test_libcephfs
    cephfs
    librados
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_libcephfs
    DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(${WITH_CEPHFS})  

