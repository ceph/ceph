if(${WITH_CEPHFS})
  add_executable(ceph_test_libcephfs
    test.cc
    readdir_r_cb.cc
    caps.cc
    multiclient.cc
    flock.cc
    acl.cc
    access.cc
    $<TARGET_OBJECTS:heap_profiler_objs>
  )
  set_target_properties(ceph_test_libcephfs PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(ceph_test_libcephfs
    cephfs
    librados
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${ALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    )
endif(${WITH_CEPHFS})  

