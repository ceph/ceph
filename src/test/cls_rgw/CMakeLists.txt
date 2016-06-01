if(${WITH_RADOSGW})
  add_executable(ceph_test_cls_rgw
    test_cls_rgw.cc
    )
  set_target_properties(ceph_test_cls_rgw PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(ceph_test_cls_rgw
    cls_rgw_client
    librados
    global
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    radostest)
endif(${WITH_RADOSGW})

