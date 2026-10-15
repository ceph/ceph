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

add_library(cls_rgw_olh_native_oracle OBJECT olh_native_oracle.cc)
target_link_libraries(cls_rgw_olh_native_oracle PRIVATE ceph-common ${FMT_LIB})
target_include_directories(cls_rgw_olh_native_oracle PRIVATE
  "${CMAKE_CURRENT_LIST_DIR}/../../rgw")
set_target_properties(cls_rgw_olh_native_oracle PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)

if(TARGET cls_rgw_datum)
  add_executable(unittest_cls_rgw_olh_wire olh_wire.cc bi_wire_main.cc
    $<TARGET_OBJECTS:cls_rgw_olh_native_oracle>
    $<TARGET_OBJECTS:cls_rgw_datum>)
  target_link_libraries(unittest_cls_rgw_olh_wire PRIVATE
    datum::zpp Catch2::Catch2 ceph-common ${FMT_LIB})
  target_include_directories(unittest_cls_rgw_olh_wire PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}/../../rgw")
  set_target_properties(unittest_cls_rgw_olh_wire PROPERTIES CXX_SCAN_FOR_MODULES OFF)
  add_test(NAME cls_rgw_olh_wire COMMAND unittest_cls_rgw_olh_wire)
endif()

add_executable(unittest_cls_rgw_olh_client olh_client.cc bi_wire_main.cc
  $<TARGET_OBJECTS:cls_rgw_olh_native_oracle>)
target_link_libraries(unittest_cls_rgw_olh_client PRIVATE
  cls_rgw_client librados global Catch2::Catch2 ceph-common ${FMT_LIB})
set_target_properties(unittest_cls_rgw_olh_client PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
add_test(NAME cls_rgw_olh_client COMMAND unittest_cls_rgw_olh_client)
