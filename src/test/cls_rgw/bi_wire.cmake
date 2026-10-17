# Focused RGW wire checks use an independently compiled C++23 native oracle.
get_filename_component(rgw_cls_source_dir "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)
add_library(cls_rgw_bi_native_oracle OBJECT "${CMAKE_CURRENT_LIST_DIR}/bi_native_oracle.cc")
target_link_libraries(cls_rgw_bi_native_oracle PRIVATE ceph-common ${FMT_LIB})
target_include_directories(cls_rgw_bi_native_oracle PRIVATE
  "${rgw_cls_source_dir}/src" "${rgw_cls_source_dir}/src/rgw")
set_target_properties(cls_rgw_bi_native_oracle PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)

add_executable(unittest_cls_rgw_bi_wire
  "${CMAKE_CURRENT_LIST_DIR}/bi_wire.cc"
  "${CMAKE_CURRENT_LIST_DIR}/bi_wire_main.cc"
  $<TARGET_OBJECTS:cls_rgw_bi_native_oracle>
  $<TARGET_OBJECTS:cls_rgw_datum>)
target_link_libraries(unittest_cls_rgw_bi_wire PRIVATE
  cls_rgw_client datum::zpp Catch2::Catch2 ceph-common ${FMT_LIB})
target_include_directories(unittest_cls_rgw_bi_wire PRIVATE
  "${rgw_cls_source_dir}/src" "${rgw_cls_source_dir}/src/rgw")
set_target_properties(unittest_cls_rgw_bi_wire PROPERTIES CXX_SCAN_FOR_MODULES OFF)
add_test(NAME cls_rgw_bi_wire COMMAND unittest_cls_rgw_bi_wire)

function(rgw_cls_add_wire_test name)
  set(target "unittest_cls_rgw_${name}_wire")
  add_executable(${target}
    "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/${name}_wire.cc"
    "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/bi_wire_main.cc")
  target_link_libraries(${target} PRIVATE
    cls_rgw_client Catch2::Catch2 ceph-common ${FMT_LIB})
  set_target_properties(${target} PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
  add_test(NAME "cls_rgw_${name}_wire" COMMAND ${target})
endfunction()

foreach(name IN ITEMS write bilog bucket index_stats object usage reshard gc lifecycle multipart)
  rgw_cls_add_wire_test(${name})
endforeach()

add_executable(unittest_cls_rgw_bi_client
  "${CMAKE_CURRENT_LIST_DIR}/bi_client.cc"
  "${CMAKE_CURRENT_LIST_DIR}/bi_wire_main.cc")
target_link_libraries(unittest_cls_rgw_bi_client PRIVATE
  cls_rgw_client librados global Catch2::Catch2 ceph-common ${FMT_LIB})
set_target_properties(unittest_cls_rgw_bi_client PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
add_test(NAME cls_rgw_bi_client COMMAND unittest_cls_rgw_bi_client)

add_executable(unittest_cls_rgw_bilog_client
  "${CMAKE_CURRENT_LIST_DIR}/bilog_client.cc"
  "${CMAKE_CURRENT_LIST_DIR}/bi_wire_main.cc")
target_link_libraries(unittest_cls_rgw_bilog_client PRIVATE
  cls_rgw_client librados global Catch2::Catch2 ceph-common ${FMT_LIB})
set_target_properties(unittest_cls_rgw_bilog_client PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
add_test(NAME cls_rgw_bilog_client COMMAND unittest_cls_rgw_bilog_client)
