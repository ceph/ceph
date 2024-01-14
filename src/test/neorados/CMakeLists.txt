add_executable(ceph_test_neorados test_neorados.cc)
target_link_libraries(ceph_test_neorados global libneorados
  ${unittest_libs}
  radostest
  radostest-cxx
  librados
  GTest::GTest)

add_executable(ceph_test_neorados_start_stop start_stop.cc)
target_link_libraries(ceph_test_neorados_start_stop global libneorados
  ${unittest_libs})

add_executable(ceph_test_neorados_completions completions.cc)
target_link_libraries(ceph_test_neorados_completions Boost::system pthread
  ${unittest_libs})

add_executable(ceph_test_neorados_op_speed op_speed.cc)
target_link_libraries(ceph_test_neorados_op_speed
  libneorados ${FMT_LIB} ${unittest_libs})

add_library(neoradostest-support STATIC common_tests.cc)
target_link_libraries(neoradostest-support
  libneorados ${FMT_LIB} GTest::GTest)

add_executable(ceph_test_neorados_list_pool list_pool.cc)
target_link_libraries(ceph_test_neorados_list_pool
  libneorados neoradostest-support global ${FMT_LIB} ${unittest_libs})

add_executable(ceph_test_neorados_handler_error
  handler_error.cc
  )
target_link_libraries(ceph_test_neorados_handler_error
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_handler_error
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_cls
  cls.cc
  )
target_link_libraries(ceph_test_neorados_cls
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_cls
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_cmd
  cmd.cc
  )
target_link_libraries(ceph_test_neorados_cmd
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_cmd
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_read_operations
  read_operations.cc
  )
target_link_libraries(ceph_test_neorados_read_operations
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_read_operations
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_write_operations
  write_operations.cc
  )
target_link_libraries(ceph_test_neorados_write_operations
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_write_operations
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_io
  io.cc
  )
target_link_libraries(ceph_test_neorados_io
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_io
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_ec_io
  ec_io.cc
  )
target_link_libraries(ceph_test_neorados_ec_io
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_ec_io
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_list
  list.cc
  )
target_link_libraries(ceph_test_neorados_list
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_list
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_misc
  misc.cc
  )
target_link_libraries(ceph_test_neorados_misc
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_misc
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_pool
  pool.cc
  )
target_link_libraries(ceph_test_neorados_pool
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_pool
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_snapshots
  snapshots.cc
  )
target_link_libraries(ceph_test_neorados_snapshots
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_snapshots
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_neorados_watch_notify
  watch_notify.cc
  )
target_link_libraries(ceph_test_neorados_watch_notify
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_watch_notify
  DESTINATION ${CMAKE_INSTALL_BINDIR})
