# Keep the Datum language requirement private to this RGW-owned CLS adapter.
add_library(cls_user_datum OBJECT cls_user_datum.cc)
target_link_libraries(cls_user_datum PRIVATE datum::zpp ${FMT_LIB})
target_include_directories(cls_user_datum PRIVATE ../.. ../../rgw)
set_target_properties(cls_user_datum PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  CXX_VISIBILITY_PRESET hidden
  CXX_SCAN_FOR_MODULES OFF)
