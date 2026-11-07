# Linux CLS plugin tests call actual handlers through an isolated storage host.
function(rgw_cls_add_module_test name source)
  set(target "unittest_cls_rgw_${name}_module")
  add_executable(${target} ${source} module_loader.cc bi_wire_main.cc)
  target_compile_definitions(${target} PRIVATE
    TEST_CLS_RGW_MODULE="$<TARGET_FILE:cls_rgw>")
  target_include_directories(${target} PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}/../../rgw")
  target_link_libraries(${target} PRIVATE
    Catch2::Catch2 ceph-common ${FMT_LIB} ${CMAKE_DL_LIBS})
  target_link_options(${target} PRIVATE -Wl,--export-dynamic)
  set_target_properties(${target} PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
  add_dependencies(${target} cls_rgw)
  add_test(NAME "cls_rgw_${name}_module" COMMAND ${target})
endfunction()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  rgw_cls_add_module_test(olh olh_module.cc)
  rgw_cls_add_module_test(bi_put bi_put_module.cc)
  target_link_libraries(unittest_cls_rgw_bi_put_module PRIVATE cls_rgw_client)
  rgw_cls_add_module_test(bi_get bi_get_module.cc)
  rgw_cls_add_module_test(object object_module.cc)
endif()

add_library(cls_rgw_olh_native_oracle OBJECT olh_native_oracle.cc)
target_link_libraries(cls_rgw_olh_native_oracle PRIVATE ceph-common ${FMT_LIB})
target_include_directories(cls_rgw_olh_native_oracle PRIVATE
  "${CMAKE_CURRENT_LIST_DIR}/../../rgw")
set_target_properties(cls_rgw_olh_native_oracle PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)

add_executable(unittest_cls_rgw_olh_wire olh_wire.cc bi_wire_main.cc
  $<TARGET_OBJECTS:cls_rgw_olh_native_oracle>
  $<TARGET_OBJECTS:cls_rgw_datum>)
target_link_libraries(unittest_cls_rgw_olh_wire PRIVATE
  cls_rgw_client datum::zpp Catch2::Catch2 ceph-common ${FMT_LIB})
target_include_directories(unittest_cls_rgw_olh_wire PRIVATE
  "${CMAKE_CURRENT_LIST_DIR}/../../rgw")
set_target_properties(unittest_cls_rgw_olh_wire PROPERTIES CXX_SCAN_FOR_MODULES OFF)
add_test(NAME cls_rgw_olh_wire COMMAND unittest_cls_rgw_olh_wire)

add_executable(unittest_cls_rgw_olh_client olh_client.cc bi_wire_main.cc
  $<TARGET_OBJECTS:cls_rgw_olh_native_oracle>)
target_link_libraries(unittest_cls_rgw_olh_client PRIVATE
  cls_rgw_client librados global Catch2::Catch2 ceph-common ${FMT_LIB})
set_target_properties(unittest_cls_rgw_olh_client PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
add_test(NAME cls_rgw_olh_client COMMAND unittest_cls_rgw_olh_client)

add_executable(unittest_cls_rgw_olh_admin olh_admin.cc bi_wire_main.cc)
target_link_libraries(unittest_cls_rgw_olh_admin PRIVATE
  cls_rgw_client Catch2::Catch2 ceph-common ${FMT_LIB})
target_include_directories(unittest_cls_rgw_olh_admin PRIVATE
  "${CMAKE_CURRENT_LIST_DIR}/../../rgw")
set_target_properties(unittest_cls_rgw_olh_admin PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
add_test(NAME cls_rgw_olh_admin COMMAND unittest_cls_rgw_olh_admin)
