# RGW-owned C++26 implementation; native client/module callers retain C++23.
add_library(cls_rgw_datum OBJECT cls_rgw_bi_datum.cc cls_rgw_olh_datum.cc)
target_link_libraries(cls_rgw_datum PRIVATE datum::zpp ${FMT_LIB})
target_include_directories(cls_rgw_datum PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/../.."
  "${CMAKE_CURRENT_SOURCE_DIR}/../../rgw")
set_target_properties(cls_rgw_datum PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  CXX_VISIBILITY_PRESET hidden
  CXX_SCAN_FOR_MODULES OFF)
