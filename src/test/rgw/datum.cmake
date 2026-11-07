# Shared by Ceph's RGW test directory and Datum's focused development build.
include(FetchContent)

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

option(DATUM_ENABLE_RAPIDCHECK "Build generated Datum algebra tests" ON)
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

add_executable(unittest_rgw_datum_core "${CMAKE_CURRENT_LIST_DIR}/datum_core.cc")
target_link_libraries(unittest_rgw_datum_core PRIVATE datum::core)
if(DATUM_ENABLE_RAPIDCHECK)
  target_link_libraries(unittest_rgw_datum_core PRIVATE rapidcheck)
  target_compile_definitions(unittest_rgw_datum_core PRIVATE DATUM_HAVE_RAPIDCHECK)
endif()

add_executable(unittest_rgw_datum_data "${CMAKE_CURRENT_LIST_DIR}/datum_data.cc")
target_link_libraries(unittest_rgw_datum_data PRIVATE datum::fmt)
if(DATUM_ENABLE_ZPP)
  target_link_libraries(unittest_rgw_datum_data PRIVATE datum::zpp)
  target_compile_definitions(unittest_rgw_datum_data PRIVATE DATUM_HAVE_ZPP)
endif()

foreach(target unittest_rgw_datum_core unittest_rgw_datum_data)
  target_include_directories(${target} PRIVATE "${CMAKE_CURRENT_LIST_DIR}/../..")
  set_target_properties(${target} PROPERTIES CXX_SCAN_FOR_MODULES OFF)
  target_compile_options(${target} PRIVATE
    "$<$<CXX_COMPILER_ID:GNU>:-Wall;-Wextra;-Werror>")

  if(COMMAND add_ceph_test)
    target_sources(${target} PRIVATE "${CMAKE_CURRENT_LIST_DIR}/../catch2_compat_main.cc")
    target_link_libraries(${target} PRIVATE Catch2::Catch2)
    add_ceph_test(${target} "$<TARGET_FILE:${target}>")
    continue()
  endif()

  target_link_libraries(${target} PRIVATE Catch2::Catch2WithMain)
  add_test(NAME ${target} COMMAND ${target})
endforeach()
