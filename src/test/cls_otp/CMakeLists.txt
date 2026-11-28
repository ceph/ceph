add_executable(ceph_test_cls_otp
  test_cls_otp.cc)
target_link_libraries(ceph_test_cls_otp
  librados
  cls_otp_client
  global
  radostest-cxx
  OATH::OATH
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )
install(TARGETS
  ceph_test_cls_otp
  DESTINATION ${CMAKE_INSTALL_BINDIR})
