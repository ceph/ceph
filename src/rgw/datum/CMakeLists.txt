# Datum is a Ceph component with a focused standalone development build.
# Keep Ceph dependency selection here, outside the pure data/algebra headers.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  cmake_minimum_required(VERSION 3.30)
  project(ceph_salad_datum LANGUAGES CXX)
  include(CTest)
  option(DATUM_BUILD_TESTING "Build Datum's tests" ${BUILD_TESTING})

endif()

option(DATUM_ENABLE_FMT "Provide the optional fmt diagnostic adapter" ON)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR AND DATUM_ENABLE_FMT)
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

include(core.cmake)

# Select the preferred native C++26 data path explicitly when it is available.
add_library(datum_reflection INTERFACE)
add_library(datum::reflection ALIAS datum_reflection)
target_link_libraries(datum_reflection INTERFACE datum::core)
# Defer feature validation until this target is consumed, so older CMake can
# still configure unrelated C++23 core consumers.
set_property(TARGET datum_reflection APPEND PROPERTY INTERFACE_COMPILE_FEATURES cxx_std_26)
target_compile_options(datum_reflection INTERFACE
  "$<$<CXX_COMPILER_ID:GNU>:-freflection>")

add_library(datum_render INTERFACE)
add_library(datum::render ALIAS datum_render)
target_link_libraries(datum_render INTERFACE datum::core)

if(DATUM_ENABLE_FMT)
  if(NOT TARGET "${FMT_LIB}")
    message(FATAL_ERROR "Datum's fmt adapter requires Ceph's configured FMT_LIB target")
  endif()
  add_library(datum_fmt INTERFACE)
  add_library(datum::fmt ALIAS datum_fmt)
  target_link_libraries(datum_fmt INTERFACE datum::render ${FMT_LIB})
endif()

# Datum's standard formatter registration is opt-in, separate from the fmt adapter.
add_library(datum_std_format INTERFACE)
add_library(datum::std_format ALIAS datum_std_format)
target_link_libraries(datum_std_format INTERFACE datum::render)
