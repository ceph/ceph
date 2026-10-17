# Datum is a Ceph component with a focused standalone development build.
# Keep Ceph dependency selection here, outside the pure data/algebra headers.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  cmake_minimum_required(VERSION 3.30)
  project(ceph_salad_datum LANGUAGES CXX)
  include(CTest)
  set(datum_testing_default ${BUILD_TESTING})

  # Direct development uses the same bundled fmt source as Ceph.
  option(WITH_FMT_HEADER_ONLY "use header-only version of fmt library" OFF)
  set(FMT_INSTALL OFF)
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../../fmt" fmt)
  if(WITH_FMT_HEADER_ONLY)
    set(FMT_LIB fmt::fmt-header-only)
  else()
    set(FMT_LIB fmt::fmt)
  endif()
else()
  set(datum_testing_default ${WITH_TESTS})
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
option(DATUM_BUILD_TESTING "Build Datum's independent tests" ${datum_testing_default})
option(DATUM_ENABLE_RAPIDCHECK "Build generated Datum algebra tests" ON)

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

if(DATUM_BUILD_TESTING)
  if(NOT TARGET Catch2::Catch2WithMain)
    find_package(Catch2 3 CONFIG QUIET)
    if(NOT TARGET Catch2::Catch2WithMain)
      FetchContent_Declare(Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG v3.8.1
        GIT_SHALLOW TRUE)
      FetchContent_MakeAvailable(Catch2)
    endif()
  endif()
  if(DATUM_ENABLE_RAPIDCHECK AND NOT TARGET rapidcheck)
    find_package(rapidcheck CONFIG QUIET)
    if(NOT TARGET rapidcheck)
      set(RC_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
      set(RC_ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
      FetchContent_Declare(rapidcheck
        GIT_REPOSITORY https://github.com/emil-e/rapidcheck.git
        GIT_TAG 6e8dadfdafa3a74eabb52ead87f8787f72eccd0b)
      FetchContent_MakeAvailable(rapidcheck)
    endif()
  endif()
endif()

if(DATUM_BUILD_TESTING)
  add_executable(datum_core_test test/test_core.cc)
  target_link_libraries(datum_core_test PRIVATE datum::core Catch2::Catch2WithMain)
  if(DATUM_ENABLE_RAPIDCHECK)
    target_link_libraries(datum_core_test PRIVATE rapidcheck)
    target_compile_definitions(datum_core_test PRIVATE DATUM_HAVE_RAPIDCHECK)
  endif()
  add_test(NAME datum_core COMMAND datum_core_test)

  add_executable(datum_data_test test/test_data.cc)
  target_link_libraries(datum_data_test PRIVATE datum::fmt Catch2::Catch2WithMain)
  if(DATUM_ENABLE_ZPP)
    target_link_libraries(datum_data_test PRIVATE datum::zpp)
    target_compile_definitions(datum_data_test PRIVATE DATUM_HAVE_ZPP)
  endif()
  add_test(NAME datum_data COMMAND datum_data_test)

  foreach(target datum_core_test datum_data_test)
    target_include_directories(${target} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../..")
    set_target_properties(${target} PROPERTIES CXX_SCAN_FOR_MODULES OFF)
    target_compile_options(${target} PRIVATE
      "$<$<CXX_COMPILER_ID:GNU>:-Wall;-Wextra;-Werror>")
  endforeach()
endif()
