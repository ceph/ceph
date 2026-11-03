# Datum is a Ceph component with a focused standalone development build.
# Keep Ceph dependency selection here, outside the pure data/algebra headers.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  cmake_minimum_required(VERSION 3.30)
  project(ceph_salad_datum LANGUAGES CXX)
  include(CTest)
  option(DATUM_BUILD_TESTING "Build Datum's tests" ${BUILD_TESTING})

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
