# ceph_test_cls_numops
add_executable(ceph_test_cls_numops 
  test_cls_numops.cc)
target_link_libraries(ceph_test_cls_numops 
  librados
  global
  cls_numops_client 
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx
  ${UNITTEST_LIBS} 
  )
install(TARGETS
  ceph_test_cls_numops
  DESTINATION ${CMAKE_INSTALL_BINDIR})
