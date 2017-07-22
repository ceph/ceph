add_executable(ceph_test_cls_lua
  test_cls_lua.cc
)
set_target_properties(ceph_test_cls_lua PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_lua
  cls_lua_client
  liblua
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  radostest
)
install(TARGETS
  ceph_test_cls_lua
  DESTINATION ${CMAKE_INSTALL_BINDIR})
