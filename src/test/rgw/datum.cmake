# Shared by Ceph's RGW test directory and Datum's focused development build.
# Other Ceph tests must remain buildable without Datum's experimental toolchain.
function(check_datum_test_support)
  if(NOT "cxx_std_26" IN_LIST CMAKE_CXX_COMPILE_FEATURES)
    set(DATUM_TEST_CORE_SUPPORTED FALSE PARENT_SCOPE)
    return()
  endif()

  include(CheckCXXSourceCompiles)
  include(CMakePushCheckState)
  cmake_push_check_state(RESET)
  set(CMAKE_CXX_STANDARD 26)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  # Probe the toolchain, so a Datum regression cannot silently disable its tests.
  check_cxx_source_compiles("
    #include <inplace_vector>
    static_assert([] {
      std::inplace_vector<int, 1> values;
      values.push_back(7);
      return 7 == values.front();
    }());
    int main() {}
  " DATUM_TEST_CORE_SUPPORTED)
  if(DATUM_TEST_CORE_SUPPORTED)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      set(CMAKE_REQUIRED_FLAGS "-freflection")
    endif()
    check_cxx_source_compiles("
      #include <array>
      #include <meta>
      struct record { int value; };
      constexpr auto fields = std::define_static_array(std::meta::nonstatic_data_members_of(
          ^^record, std::meta::access_context::unprivileged()));
      static_assert(1 == fields.size());
      struct projection;
      consteval {
        std::meta::define_aggregate(^^projection,
            std::array {std::meta::data_member_spec(^^int, {.name = \"value\"})});
      }
      constexpr projection projected {7};
      static_assert(7 == projected.value);
      static_assert([] {
        int count = 0;
        template for (constexpr auto field : fields) {
          count += std::meta::is_nonstatic_data_member(field);
        }
        return 1 == count;
      }());
      int main() {}
    " DATUM_TEST_DATA_SUPPORTED)
  endif()
  cmake_pop_check_state()
endfunction()

check_datum_test_support()
if(NOT DATUM_TEST_CORE_SUPPORTED)
  if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    message(FATAL_ERROR "Datum tests require C++26 language and library support")
  endif()
  message(STATUS "Skipping Datum tests: C++26 language or library support is unavailable")
  return()
endif()
if(NOT DATUM_TEST_DATA_SUPPORTED)
  if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    message(FATAL_ERROR "Datum data tests require C++26 reflection support")
  endif()
  message(STATUS "Skipping Datum data tests: C++26 reflection support is unavailable")
endif()

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
set(datum_tests unittest_rgw_datum_core)
if(DATUM_ENABLE_RAPIDCHECK)
  target_link_libraries(unittest_rgw_datum_core PRIVATE rapidcheck)
  target_compile_definitions(unittest_rgw_datum_core PRIVATE DATUM_HAVE_RAPIDCHECK)
endif()

if(DATUM_TEST_DATA_SUPPORTED)
  add_executable(unittest_rgw_datum_data "${CMAKE_CURRENT_LIST_DIR}/datum_data.cc")
  target_link_libraries(unittest_rgw_datum_data PRIVATE datum::fmt)
  if(DATUM_ENABLE_ZPP)
    target_link_libraries(unittest_rgw_datum_data PRIVATE datum::zpp)
    target_compile_definitions(unittest_rgw_datum_data PRIVATE DATUM_HAVE_ZPP)
  endif()
  list(APPEND datum_tests unittest_rgw_datum_data)
endif()

foreach(target IN LISTS datum_tests)
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
