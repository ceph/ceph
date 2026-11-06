cmake_minimum_required(VERSION 3.30)
project(datum_tutorials LANGUAGES CXX)

set(DATUM_SOURCE_DIR "" CACHE PATH "Path to Datum's src/rgw/datum directory")
if(NOT EXISTS "${DATUM_SOURCE_DIR}/core.cmake")
  message(FATAL_ERROR "DATUM_SOURCE_DIR must name a Datum directory containing core.cmake")
endif()
include("${DATUM_SOURCE_DIR}/core.cmake")

# Probe the optional language facility, rather than requiring it for byte lessons.
include(CheckCXXSourceCompiles)
include(CMakePushCheckState)
if("cxx_std_26" IN_LIST CMAKE_CXX_COMPILE_FEATURES)
  cmake_push_check_state(RESET)
  set(CMAKE_CXX_STANDARD 26)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_REQUIRED_FLAGS "-freflection")
  endif()
  check_cxx_source_compiles("
    #include <meta>
    constexpr auto reflected = ^^int;
    int main() { return 0; }
  " DATUM_TUTORIAL_HAS_REFLECTION)
  cmake_pop_check_state()
  unset(CMAKE_CXX_STANDARD)
  unset(CMAKE_CXX_STANDARD_REQUIRED)
endif()
option(DATUM_TUTORIAL_BUILD_REFLECTION "Use the C++26 backend for record-view examples"
  ${DATUM_TUTORIAL_HAS_REFLECTION})
if(DATUM_TUTORIAL_BUILD_REFLECTION)
  if(NOT DATUM_TUTORIAL_HAS_REFLECTION)
    message(FATAL_ERROR "The native record backend requires a compiler with C++26 reflection")
  endif()
  add_library(datum_tutorial_reflection INTERFACE)
  target_link_libraries(datum_tutorial_reflection INTERFACE datum::core)
  target_compile_features(datum_tutorial_reflection INTERFACE cxx_std_26)
  target_compile_options(datum_tutorial_reflection INTERFACE
    "$<$<CXX_COMPILER_ID:GNU>:-freflection>")
endif()

include(CTest)
if(BUILD_TESTING)
  add_custom_target(check
    COMMAND "${CMAKE_CTEST_COMMAND}" --output-on-failure -C "$<CONFIG>"
    USES_TERMINAL)
endif()

function(add_datum_example name expected_output)
  add_executable(${name} "examples/${name}.cpp")
  target_link_libraries(${name} PRIVATE datum::core ${ARGN})
  target_compile_features(${name} PRIVATE cxx_std_23)
  set_target_properties(${name} PROPERTIES
    CXX_EXTENSIONS OFF
    CXX_SCAN_FOR_MODULES OFF)
  target_compile_options(${name} PRIVATE
    "$<$<CXX_COMPILER_ID:GNU,Clang>:-Wall;-Wextra;-Wpedantic>")

  if(BUILD_TESTING)
    add_test(NAME ${name}
      COMMAND "${CMAKE_COMMAND}"
        "-DEXECUTABLE=$<TARGET_FILE:${name}>"
        "-DEXPECTED_OUTPUTS=${PROJECT_SOURCE_DIR}/expected.json"
        "-DOUTPUT_NAME=${expected_output}"
        -P "${PROJECT_SOURCE_DIR}/cmake/CheckOutput.cmake")
    add_dependencies(check ${name})
  endif()
endfunction()

add_datum_example(read_plan read_plan)
add_datum_example(developer_flow developer_flow)
add_datum_example(01_hello_fragments 01_hello_fragments)
add_datum_example(01_hello_fragments_variation 01_hello_fragments)
add_datum_example(02_seat_selection 02_seat_selection)
add_datum_example(02_seat_selection_variation 02_seat_selection_variation)
add_datum_example(02_half_interval_interlude 02_half_interval_interlude)
add_datum_example(02_half_interval_interlude_variation 02_half_interval_interlude_variation)
add_datum_example(03_normalize_ranges 03_normalize_ranges)
add_datum_example(03_normalize_ranges_variation 03_normalize_ranges_variation)
add_datum_example(04_numeric_keys 04_numeric_keys)
add_datum_example(04_numeric_keys_variation 04_numeric_keys_variation)
add_datum_example(05_labeled_slices 05_labeled_slices)
add_datum_example(05_labeled_slices_variation 05_labeled_slices_variation)
add_datum_example(06_covered_quota 06_covered_quota)
add_datum_example(06_covered_quota_variation 06_covered_quota_variation)
add_datum_example(07_retained_slice 07_retained_slice)
add_datum_example(07_retained_slice_variation 07_retained_slice_variation)
add_datum_example(08_toy_basket 08_toy_basket)
add_datum_example(08_toy_basket_variation 08_toy_basket_variation)
if(DATUM_TUTORIAL_BUILD_REFLECTION)
  target_link_libraries(08_toy_basket PRIVATE datum_tutorial_reflection)
  target_link_libraries(08_toy_basket_variation PRIVATE datum_tutorial_reflection)
endif()
add_datum_example(09_selected_fields 09_selected_fields)
add_datum_example(09_selected_fields_variation 09_selected_fields_variation)
add_datum_example(10_scan_resume 10_scan_resume)
add_datum_example(10_scan_resume_variation 10_scan_resume_variation)
add_datum_example(11_discreet_log 11_discreet_log)
add_datum_example(11_discreet_log_variation 11_discreet_log_variation)

option(DATUM_TUTORIAL_BUILD_CODECS "Build examples using Datum's optional codec adapter" OFF)
option(DATUM_TUTORIAL_BUILD_RGW "Build the native RGW examples" OFF)
option(DATUM_TUTORIAL_BUILD_CEPH "Build the shared Ceph vocabulary lesson" OFF)
if(DATUM_TUTORIAL_BUILD_RGW AND NOT DATUM_TUTORIAL_BUILD_REFLECTION)
  message(FATAL_ERROR "The RGW preview requires DATUM_TUTORIAL_BUILD_REFLECTION")
endif()
if(DATUM_TUTORIAL_BUILD_CODECS OR DATUM_TUTORIAL_BUILD_RGW)
  include(FetchContent)
  FetchContent_Declare(zpp_bits
    GIT_REPOSITORY https://github.com/eyalz800/zpp_bits.git
    GIT_TAG 4a57ecd68a481c2cd6e7ee94aaf83282a735fded) # v4.7.6
  FetchContent_MakeAvailable(zpp_bits)

  # zpp_bits provides a header directory, without an upstream CMake target.
  add_library(datum_tutorial_zpp INTERFACE)
  target_link_libraries(datum_tutorial_zpp INTERFACE datum::core)
  target_include_directories(datum_tutorial_zpp SYSTEM INTERFACE "${zpp_bits_SOURCE_DIR}")
endif()

if(DATUM_TUTORIAL_BUILD_CEPH)
  include(cmake/CephExample.cmake)
  foreach(target IN ITEMS 18_ceph_bytes 18_ceph_bytes_variation)
    add_datum_example(${target} ${target} datum_tutorial_ceph)
    set_property(TARGET ${target} APPEND PROPERTY BUILD_RPATH "${CEPH_BUILD_DIR}/lib")
  endforeach()
endif()

if(DATUM_TUTORIAL_BUILD_CODECS)
  add_datum_example(12_postcard 12_postcard datum_tutorial_zpp)
  add_datum_example(12_postcard_variation 12_postcard_variation datum_tutorial_zpp)
endif()

add_datum_example(13_three_tickets 13_three_tickets)
add_datum_example(13_three_tickets_variation 13_three_tickets_variation)
add_datum_example(14_turn_page 14_turn_page)
add_datum_example(14_turn_page_variation 14_turn_page_variation)
add_datum_example(15_order_ticket 15_order_ticket)
add_datum_example(15_order_ticket_variation 15_order_ticket_variation)

if(DATUM_TUTORIAL_BUILD_RGW)
  include(cmake/RgwExample.cmake)
  foreach(target IN ITEMS
      16_rgw_page 16_rgw_page_variation
      17_bucket_listing 17_bucket_listing_variation)
    add_datum_example(${target} ${target} datum_tutorial_rgw)
    set_property(TARGET ${target} APPEND PROPERTY BUILD_RPATH "${CEPH_BUILD_DIR}/lib")
  endforeach()
endif()
