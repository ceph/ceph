# Native adapters come from the separate RGW integration, alongside its Ceph headers.
set(CEPH_SOURCE_DIR "" CACHE PATH "Native RGW integration source tree (separate from Datum)")
set(CEPH_BUILD_DIR "" CACHE PATH "Compatible Ceph build with generated headers and libraries")
foreach(required IN ITEMS
    "${CEPH_SOURCE_DIR}/src/rgw/datum/adapters/cls/rgw.cc"
    "${CEPH_SOURCE_DIR}/src/cls/rgw/cls_rgw_ops.h"
    "${CEPH_BUILD_DIR}/include/acconfig.h")
  if(NOT EXISTS "${required}")
    message(FATAL_ERROR "The native RGW integration requires ${required}")
  endif()
endforeach()

find_path(DATUM_TUTORIAL_BOOST_INCLUDE_DIR boost/container/flat_map.hpp
  HINTS "${CEPH_BUILD_DIR}/boost/src/Boost" REQUIRED)
find_path(DATUM_TUTORIAL_FMT_INCLUDE_DIR fmt/format.h
  HINTS "${CEPH_SOURCE_DIR}/src/fmt/include" NO_DEFAULT_PATH REQUIRED)
find_library(DATUM_TUTORIAL_CEPH_COMMON ceph-common
  HINTS "${CEPH_BUILD_DIR}/lib" NO_DEFAULT_PATH REQUIRED)
find_library(DATUM_TUTORIAL_CEPH_FMT fmt
  HINTS "${CEPH_BUILD_DIR}/lib" NO_DEFAULT_PATH REQUIRED)

add_library(datum_tutorial_rgw STATIC "${CEPH_SOURCE_DIR}/src/rgw/datum/adapters/cls/rgw.cc")
target_link_libraries(datum_tutorial_rgw
  PUBLIC datum::core "${DATUM_TUTORIAL_CEPH_COMMON}"
  PRIVATE datum_tutorial_zpp "${DATUM_TUTORIAL_CEPH_FMT}")
target_include_directories(datum_tutorial_rgw SYSTEM PUBLIC
  "${CEPH_BUILD_DIR}/src/include"
  "${CEPH_BUILD_DIR}/include"
  "${CEPH_SOURCE_DIR}/src"
  "${CEPH_SOURCE_DIR}/src/rgw"
  "${CEPH_SOURCE_DIR}/src/rgw/driver/rados"
  "${DATUM_TUTORIAL_BOOST_INCLUDE_DIR}"
  "${DATUM_TUTORIAL_FMT_INCLUDE_DIR}")
target_compile_definitions(datum_tutorial_rgw PUBLIC HAVE_CONFIG_H __CEPH__)

# The shared binding source also defines RGW operations this lesson does not use.
target_compile_options(datum_tutorial_rgw PRIVATE
  "$<$<CXX_COMPILER_ID:GNU,Clang>:-ffunction-sections;-fdata-sections>")
target_link_options(datum_tutorial_rgw INTERFACE
  "$<$<CXX_COMPILER_ID:GNU,Clang>:-Wl,--gc-sections>")
set_target_properties(datum_tutorial_rgw PROPERTIES
  CXX_EXTENSIONS OFF
  CXX_SCAN_FOR_MODULES OFF)
