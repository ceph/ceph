# RGW-owned C++26 implementation; native client/module callers retain C++23.
add_library(cls_rgw_datum OBJECT cls_rgw_bi_datum.cc cls_rgw_olh_datum.cc)
add_library(cls_rgw_datum_gc OBJECT cls_rgw_gc_datum.cc)
foreach(target cls_rgw_datum cls_rgw_datum_gc)
  target_link_libraries(${target} PRIVATE datum::zpp ${FMT_LIB})
  target_include_directories(${target} PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../.."
    "${CMAKE_CURRENT_SOURCE_DIR}/../../rgw")
  set_target_properties(${target} PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    CXX_VISIBILITY_PRESET hidden
    CXX_SCAN_FOR_MODULES OFF)
endforeach()
