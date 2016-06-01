# ceph_test_cls_numops
add_executable(ceph_test_cls_numops 
  test_cls_numops.cc)
set_target_properties(ceph_test_cls_numops PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_numops 
  librados 
  global
  cls_numops_client 
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest
  ${UNITTEST_LIBS} 
  )

