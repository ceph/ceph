# cls_test_cls_rbd
add_executable(ceph_test_cls_rbd
  test_cls_rbd.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  $<TARGET_OBJECTS:parse_secret_objs>
  )
set_target_properties(ceph_test_cls_rbd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_rbd
  librbd
  cls_rbd
  cls_lock
  librados
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  blkid
  udev
  keyutils
  )

