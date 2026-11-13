# Native bindings use Datum's C++23 core and keep codec requirements private.
add_library(cls_rgw_datum OBJECT cls/rgw.cc cls/olh.cc)
add_library(cls_rgw_datum_gc OBJECT cls/gc.cc)
add_library(cls_user_datum OBJECT cls/user.cc)
foreach(target cls_rgw_datum cls_rgw_datum_gc cls_user_datum)
  target_link_libraries(${target} PRIVATE datum::ceph datum::zpp ${FMT_LIB})
  target_include_directories(${target} PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../..")
  set_target_properties(${target} PROPERTIES
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
    CXX_VISIBILITY_PRESET hidden
    CXX_SCAN_FOR_MODULES OFF)
endforeach()
