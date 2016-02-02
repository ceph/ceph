# ceph_test_cls_version
add_executable(ceph_test_cls_version
  test_cls_version.cc
  )
set_target_properties(ceph_test_cls_version PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_version
  librados
  cls_version_client
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

