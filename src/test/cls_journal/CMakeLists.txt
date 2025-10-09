# cls_test_cls_journal
add_executable(ceph_test_cls_journal
  test_cls_journal.cc
  $<TARGET_OBJECTS:common_texttable_obj>)
target_link_libraries(ceph_test_cls_journal
  cls_journal_client
  librados
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest-cxx)
install(TARGETS
  ceph_test_cls_journal
  DESTINATION ${CMAKE_INSTALL_BINDIR})
