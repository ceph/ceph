# Linux CLS plugin tests call actual handlers through an isolated storage host.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(unittest_cls_rgw_olh_module olh_module.cc bi_wire_main.cc)
  target_compile_definitions(unittest_cls_rgw_olh_module PRIVATE
    TEST_CLS_RGW_MODULE="$<TARGET_FILE:cls_rgw>")
  target_include_directories(unittest_cls_rgw_olh_module PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}/../../rgw")
  target_link_libraries(unittest_cls_rgw_olh_module PRIVATE
    Catch2::Catch2 ceph-common ${FMT_LIB} ${CMAKE_DL_LIBS})
  target_link_options(unittest_cls_rgw_olh_module PRIVATE -Wl,--export-dynamic)
  set_target_properties(unittest_cls_rgw_olh_module PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
  add_dependencies(unittest_cls_rgw_olh_module cls_rgw)
  add_test(NAME cls_rgw_olh_module COMMAND unittest_cls_rgw_olh_module)
endif()
