if(TARGET cls_rgw_bi_datum)
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
    $<TARGET_OBJECTS:cls_rgw_bi_datum>)
  target_link_libraries(unittest_cls_rgw_bi_wire PRIVATE
    datum::zpp Catch2::Catch2 ceph-common ${FMT_LIB})
  target_include_directories(unittest_cls_rgw_bi_wire PRIVATE
    "${rgw_cls_source_dir}/src" "${rgw_cls_source_dir}/src/rgw")
  set_target_properties(unittest_cls_rgw_bi_wire PROPERTIES CXX_SCAN_FOR_MODULES OFF)
  add_test(NAME cls_rgw_bi_wire COMMAND unittest_cls_rgw_bi_wire)
endif()

add_executable(unittest_cls_rgw_bi_client
  "${CMAKE_CURRENT_LIST_DIR}/bi_client.cc"
  "${CMAKE_CURRENT_LIST_DIR}/bi_wire_main.cc")
target_link_libraries(unittest_cls_rgw_bi_client PRIVATE
  cls_rgw_client librados global Catch2::Catch2 ceph-common ${FMT_LIB})
set_target_properties(unittest_cls_rgw_bi_client PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
add_test(NAME cls_rgw_bi_client COMMAND unittest_cls_rgw_bi_client)
