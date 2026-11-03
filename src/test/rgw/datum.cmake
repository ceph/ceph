# Shared by Ceph's RGW test directory and Datum's focused development build.
# Other Ceph tests must remain buildable without Datum's experimental toolchain.
function(check_datum_test_support)
  if(NOT "cxx_std_23" IN_LIST CMAKE_CXX_COMPILE_FEATURES)
    set(DATUM_TEST_CORE_SUPPORTED FALSE PARENT_SCOPE)
    return()
  endif()

  include(CheckCXXSourceCompiles)
  include(CMakePushCheckState)
  cmake_push_check_state(RESET)
  set(CMAKE_CXX_STANDARD 23)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  # Probe only the toolchain; a Datum regression must fail its real tests.
  # Native stop tokens are optional; test the fallback's standard-library primitives.
  # Use a new cache key so an earlier native-stop-token failure cannot persist.
  check_cxx_source_compiles("
    #include <atomic>
    #include <expected>
    #include <memory>
    static_assert(7 == *std::expected<int, int> {7});
    int main() {
      const auto state = std::make_shared<std::atomic<bool>>(false);
      return state->exchange(true, std::memory_order_acq_rel);
    }
  " DATUM_TEST_CXX23_TOOLCHAIN_SUPPORTED)
  # Some Clang/libstdc++ combinations provide std::expected but reject even a
  # standard-only constexpr string move. Keep Datum's strong assertions intact:
  # this probe diagnoses the library limitation without compiling Datum headers.
  check_cxx_source_compiles("
    #include <string>
    #include <utility>
    static_assert([] {
      std::string source;
      source.append(\"Datum\");
      const auto copied = source;
      const auto moved = std::move(source);
      return copied == moved && moved == \"Datum\";
    }());
    int main() {}
  " DATUM_TEST_CONSTEXPR_STRING_SUPPORTED)
  if(DATUM_TEST_CXX23_TOOLCHAIN_SUPPORTED AND DATUM_TEST_CONSTEXPR_STRING_SUPPORTED)
    set(DATUM_TEST_CORE_SUPPORTED TRUE PARENT_SCOPE)
  else()
    set(DATUM_TEST_CORE_SUPPORTED FALSE PARENT_SCOPE)
  endif()
  # Query only the optional standard backend, never Datum's implementation.
  check_cxx_source_compiles("
    #include <format>
    static_assert(std::formattable<int, char>);
    int main() { return std::format(\"{}\", 7) != \"7\"; }
  " DATUM_TEST_STD_FORMAT_SUPPORTED)
  if(DATUM_TEST_CXX23_TOOLCHAIN_SUPPORTED AND "cxx_std_26" IN_LIST CMAKE_CXX_COMPILE_FEATURES)
    set(CMAKE_CXX_STANDARD 26)
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
  else()
    set(DATUM_TEST_DATA_SUPPORTED FALSE PARENT_SCOPE)
  endif()
  cmake_pop_check_state()
endfunction()

check_datum_test_support()
option(DATUM_REQUIRE_REFLECTION_TESTS "Require the stronger C++26 reflection test suite" OFF)
# Temporary C++23: report runtime qualification separately from the constexpr gate.
option(DATUM_RUNTIME_QUALIFICATION_ONLY
  "Select runtime qualification without the independent constexpr check" OFF)
if(NOT DATUM_TEST_CORE_SUPPORTED)
  if(DATUM_REQUIRE_REFLECTION_TESTS OR CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    message(FATAL_ERROR "Datum core tests require C++23, std::expected, atomics, shared ownership, "
      "and constexpr std::string construction, copying, and moving")
  endif()
  message(STATUS "Skipping Datum core tests: required C++23 toolchain facilities are unavailable "
    "(including constexpr std::string construction, copying, and moving)")
  return()
endif()
if(NOT DATUM_TEST_DATA_SUPPORTED)
  if(DATUM_REQUIRE_REFLECTION_TESTS)
    message(FATAL_ERROR "Datum data tests require C++26 reflection support")
  endif()
  message(STATUS "Datum reflection tests unavailable: C++26 reflection is required; "
    "core tests remain enabled")
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

set(datum_core_test unittest_rgw_datum_core)
set(datum_core_labels datum constexpr)
if(DATUM_RUNTIME_QUALIFICATION_ONLY)
  set(datum_core_test unittest_rgw_datum_runtime)
  set(datum_core_labels datum runtime-qualification)
  message(STATUS "Datum runtime qualification selected: the independent constexpr check is not included")
endif()

add_executable(${datum_core_test}
  "${CMAKE_CURRENT_LIST_DIR}/datum_core.cc"
  "${CMAKE_CURRENT_LIST_DIR}/datum_portable.cc")
if(NOT DATUM_RUNTIME_QUALIFICATION_ONLY)
  target_sources(${datum_core_test} PRIVATE "${CMAKE_CURRENT_LIST_DIR}/datum_constexpr.cc")
endif()
target_link_libraries(${datum_core_test} PRIVATE datum::core)
set(datum_tests ${datum_core_test})
if(DATUM_ENABLE_RAPIDCHECK)
  target_link_libraries(${datum_core_test} PRIVATE rapidcheck)
  target_compile_definitions(${datum_core_test} PRIVATE DATUM_HAVE_RAPIDCHECK)
endif()

add_executable(unittest_rgw_datum_ownership "${CMAKE_CURRENT_LIST_DIR}/datum_ownership.cc")
target_link_libraries(unittest_rgw_datum_ownership PRIVATE datum::core)
list(APPEND datum_tests unittest_rgw_datum_ownership)

add_executable(unittest_rgw_datum_ranges "${CMAKE_CURRENT_LIST_DIR}/datum_ranges.cc")
target_link_libraries(unittest_rgw_datum_ranges PRIVATE datum::core)
list(APPEND datum_tests unittest_rgw_datum_ranges)

add_executable(unittest_rgw_datum_vocabulary "${CMAKE_CURRENT_LIST_DIR}/datum_vocabulary.cc")
target_link_libraries(unittest_rgw_datum_vocabulary PRIVATE datum::core)
list(APPEND datum_tests unittest_rgw_datum_vocabulary)

# Native tests join Ceph's existing dependency graph; standalone core stays independent.
if(TARGET datum::ceph AND TARGET ceph-common)
  add_executable(unittest_rgw_datum_ceph "${CMAKE_CURRENT_LIST_DIR}/datum_ceph.cc")
  target_link_libraries(unittest_rgw_datum_ceph PRIVATE datum::ceph)
  list(APPEND datum_tests unittest_rgw_datum_ceph)
  if(DATUM_ENABLE_ZPP)
    add_executable(unittest_rgw_datum_ceph_wire "${CMAKE_CURRENT_LIST_DIR}/datum_ceph_wire.cc")
    target_link_libraries(unittest_rgw_datum_ceph_wire PRIVATE
      datum::ceph datum::zpp)
    target_include_directories(unittest_rgw_datum_ceph_wire SYSTEM PRIVATE
      "${CMAKE_CURRENT_LIST_DIR}/../..")
    list(APPEND datum_tests unittest_rgw_datum_ceph_wire)
  endif()
endif()

add_executable(unittest_rgw_datum_definitions "${CMAKE_CURRENT_LIST_DIR}/datum_definitions.cc")
target_link_libraries(unittest_rgw_datum_definitions PRIVATE datum::core)
list(APPEND datum_tests unittest_rgw_datum_definitions)

add_executable(unittest_rgw_datum_views "${CMAKE_CURRENT_LIST_DIR}/datum_views.cc")
target_link_libraries(unittest_rgw_datum_views PRIVATE datum::core)
list(APPEND datum_tests unittest_rgw_datum_views)

add_executable(unittest_rgw_datum_fields "${CMAKE_CURRENT_LIST_DIR}/datum_fields.cc")
target_link_libraries(unittest_rgw_datum_fields PRIVATE datum::core)
list(APPEND datum_tests unittest_rgw_datum_fields)

add_executable(unittest_rgw_datum_schema "${CMAKE_CURRENT_LIST_DIR}/datum_schema.cc")
target_link_libraries(unittest_rgw_datum_schema PRIVATE datum::core)
list(APPEND datum_tests unittest_rgw_datum_schema)

if(DATUM_ENABLE_ZPP)
  add_executable(unittest_rgw_datum_codec
    "${CMAKE_CURRENT_LIST_DIR}/datum_codec.cc"
    "${CMAKE_CURRENT_LIST_DIR}/datum_codec_pipeline.cc")
  target_link_libraries(unittest_rgw_datum_codec PRIVATE datum::zpp datum::render)
  list(APPEND datum_tests unittest_rgw_datum_codec)
endif()

add_executable(unittest_rgw_datum_render "${CMAKE_CURRENT_LIST_DIR}/datum_render.cc")
target_link_libraries(unittest_rgw_datum_render PRIVATE datum::render)
list(APPEND datum_tests unittest_rgw_datum_render)

add_executable(unittest_rgw_datum_display_profiles "${CMAKE_CURRENT_LIST_DIR}/datum_display_profiles.cc")
target_link_libraries(unittest_rgw_datum_display_profiles PRIVATE datum::render)
list(APPEND datum_tests unittest_rgw_datum_display_profiles)

if(TARGET datum::fmt)
  add_executable(unittest_rgw_datum_render_adapters
    "${CMAKE_CURRENT_LIST_DIR}/datum_render_adapters.cc"
    "${CMAKE_CURRENT_LIST_DIR}/datum_display_profiles_format.cc")
  target_link_libraries(unittest_rgw_datum_render_adapters PRIVATE datum::fmt)
  list(APPEND datum_tests unittest_rgw_datum_render_adapters)
endif()

if(DATUM_TEST_STD_FORMAT_SUPPORTED)
  add_executable(unittest_rgw_datum_std_format
    "${CMAKE_CURRENT_LIST_DIR}/datum_std_format.cc"
    "${CMAKE_CURRENT_LIST_DIR}/datum_display_profiles_format.cc")
  target_compile_definitions(unittest_rgw_datum_std_format PRIVATE DATUM_TEST_STANDARD_FORMAT)
  target_link_libraries(unittest_rgw_datum_std_format PRIVATE datum::std_format)
  list(APPEND datum_tests unittest_rgw_datum_std_format)
endif()

if(DATUM_TEST_DATA_SUPPORTED AND TARGET datum::fmt AND DATUM_TEST_STD_FORMAT_SUPPORTED)
  add_executable(unittest_rgw_datum_data "${CMAKE_CURRENT_LIST_DIR}/datum_data.cc")
  target_link_libraries(unittest_rgw_datum_data PRIVATE datum::reflection datum::fmt datum::std_format)
  if(DATUM_ENABLE_ZPP)
    target_link_libraries(unittest_rgw_datum_data PRIVATE datum::zpp)
    target_compile_definitions(unittest_rgw_datum_data PRIVATE DATUM_HAVE_ZPP)
  endif()
  list(APPEND datum_tests unittest_rgw_datum_data)
elseif(DATUM_TEST_DATA_SUPPORTED)
  if(DATUM_REQUIRE_REFLECTION_TESTS)
    message(FATAL_ERROR "The combined native Datum tests require fmt and standard formatting support")
  endif()
  message(STATUS
    "Datum combined native tests unavailable: fmt and standard formatting are required; "
    "portable tests remain enabled")
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

set_tests_properties(${datum_core_test} PROPERTIES LABELS "${datum_core_labels}")
set_tests_properties(unittest_rgw_datum_ownership PROPERTIES LABELS "datum;ownership")
set_tests_properties(unittest_rgw_datum_ranges PROPERTIES LABELS "datum;range;scan")
set_tests_properties(unittest_rgw_datum_vocabulary PROPERTIES LABELS "datum;vocabulary;range")

set_tests_properties(unittest_rgw_datum_definitions PROPERTIES LABELS "datum;record-definition")

set_tests_properties(unittest_rgw_datum_views PROPERTIES LABELS "datum;view")

set_tests_properties(unittest_rgw_datum_fields PROPERTIES LABELS "datum;fields")

set_tests_properties(unittest_rgw_datum_schema PROPERTIES LABELS "datum;schema")

if(TARGET unittest_rgw_datum_codec)
  set_tests_properties(unittest_rgw_datum_codec PROPERTIES LABELS "datum;codec")
endif()

if(TARGET unittest_rgw_datum_render_adapters)
  set_tests_properties(unittest_rgw_datum_render_adapters PROPERTIES LABELS "datum;render;adapter")
endif()

set_tests_properties(unittest_rgw_datum_render PROPERTIES LABELS "datum;render")
set_tests_properties(unittest_rgw_datum_display_profiles PROPERTIES LABELS "datum;render;profile")
if(TARGET unittest_rgw_datum_std_format)
  set_tests_properties(unittest_rgw_datum_std_format PROPERTIES LABELS "datum;render;std-format")
endif()

if(TARGET unittest_rgw_datum_data)
  set_tests_properties(unittest_rgw_datum_data PROPERTIES LABELS "datum;reflection")
endif()

if(TARGET unittest_rgw_datum_ceph)
  set_tests_properties(unittest_rgw_datum_ceph PROPERTIES LABELS "datum;ceph;range")
endif()
if(TARGET unittest_rgw_datum_ceph_wire)
  set_tests_properties(unittest_rgw_datum_ceph_wire PROPERTIES LABELS "datum;ceph;wire")
endif()
