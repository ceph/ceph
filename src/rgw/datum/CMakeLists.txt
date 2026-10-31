# Datum is a Ceph component with a focused standalone development build.
# Keep Ceph dependency selection here, outside the pure data/algebra headers.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  cmake_minimum_required(VERSION 3.30)
  project(ceph_salad_datum LANGUAGES CXX)
  include(CTest)
  option(DATUM_BUILD_TESTING "Build Datum's tests" ${BUILD_TESTING})

  # Direct development uses the same bundled fmt source as Ceph.
  option(WITH_FMT_HEADER_ONLY "use header-only version of fmt library" OFF)
  set(FMT_INSTALL OFF)
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../../fmt" fmt)
  if(WITH_FMT_HEADER_ONLY)
    set(FMT_LIB fmt::fmt-header-only)
  else()
    set(FMT_LIB fmt::fmt)
  endif()
endif()

if(NOT TARGET "${FMT_LIB}")
  message(FATAL_ERROR "Datum requires Ceph's configured fmt target (FMT_LIB)")
endif()

include(core.cmake)

# Reflection is needed by ownership and schema facilities, not by the algebra.
add_library(datum_reflection INTERFACE)
add_library(datum::reflection ALIAS datum_reflection)
target_link_libraries(datum_reflection INTERFACE datum::core)
target_compile_options(datum_reflection INTERFACE
  "$<$<CXX_COMPILER_ID:GNU>:-freflection>")

option(DATUM_ENABLE_ZPP "Provide the optional zpp codec adapter" ON)

include(FetchContent)
if(DATUM_ENABLE_ZPP)
  if(NOT TARGET zpp_bits)
    FetchContent_Declare(zpp_bits
      GIT_REPOSITORY https://github.com/eyalz800/zpp_bits.git
      GIT_TAG v4.5.1
      GIT_SHALLOW TRUE)
    FetchContent_MakeAvailable(zpp_bits)
    if(NOT TARGET zpp_bits)
      add_library(zpp_bits INTERFACE)
      target_include_directories(zpp_bits SYSTEM INTERFACE "${zpp_bits_SOURCE_DIR}")
    endif()
  endif()
  add_library(datum_zpp INTERFACE)
  add_library(datum::zpp ALIAS datum_zpp)
  target_link_libraries(datum_zpp INTERFACE datum::reflection zpp_bits)
endif()

add_library(datum_fmt INTERFACE)
add_library(datum::fmt ALIAS datum_fmt)
target_link_libraries(datum_fmt INTERFACE datum::reflection ${FMT_LIB})

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR AND DATUM_BUILD_TESTING)
  include("${CMAKE_CURRENT_SOURCE_DIR}/../../test/rgw/datum.cmake")
endif()
