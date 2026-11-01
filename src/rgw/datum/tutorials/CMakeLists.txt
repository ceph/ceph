cmake_minimum_required(VERSION 3.30)
project(datum_tutorials LANGUAGES CXX)

set(DATUM_SOURCE_DIR "" CACHE PATH "Path to Datum's src/rgw/datum directory")
if(NOT EXISTS "${DATUM_SOURCE_DIR}/core.cmake")
  message(FATAL_ERROR "DATUM_SOURCE_DIR must name a Datum directory containing core.cmake")
endif()
include("${DATUM_SOURCE_DIR}/core.cmake")

# Import reflection requirements without Datum's full entry and its fmt setup.
add_library(datum_tutorial_reflection INTERFACE)
target_link_libraries(datum_tutorial_reflection INTERFACE datum::core)
target_compile_options(datum_tutorial_reflection INTERFACE
  "$<$<CXX_COMPILER_ID:GNU>:-freflection>")

include(CTest)
if(BUILD_TESTING)
  add_custom_target(check
    COMMAND "${CMAKE_CTEST_COMMAND}" --output-on-failure -C "$<CONFIG>"
    USES_TERMINAL)
endif()

function(add_datum_example name expected_output)
  add_executable(${name} "examples/${name}.cpp")
  target_link_libraries(${name} PRIVATE datum::core ${ARGN})
  set_target_properties(${name} PROPERTIES
    CXX_EXTENSIONS OFF
    CXX_SCAN_FOR_MODULES OFF)
  target_compile_options(${name} PRIVATE
    "$<$<CXX_COMPILER_ID:GNU,Clang>:-Wall;-Wextra;-Wpedantic>")

  if(BUILD_TESTING)
    add_test(NAME ${name}
      COMMAND "${CMAKE_COMMAND}"
        "-DEXECUTABLE=$<TARGET_FILE:${name}>"
        "-DEXPECTED_OUTPUT=${PROJECT_SOURCE_DIR}/expected/${expected_output}.txt"
        -P "${PROJECT_SOURCE_DIR}/cmake/CheckOutput.cmake")
    add_dependencies(check ${name})
  endif()
endfunction()

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
add_datum_example(07_retained_slice 07_retained_slice datum_tutorial_reflection)
add_datum_example(07_retained_slice_variation 07_retained_slice_variation datum_tutorial_reflection)
add_datum_example(08_toy_basket 08_toy_basket datum_tutorial_reflection)
add_datum_example(08_toy_basket_variation 08_toy_basket_variation datum_tutorial_reflection)
add_datum_example(09_selected_fields 09_selected_fields datum_tutorial_reflection)
add_datum_example(09_selected_fields_variation 09_selected_fields_variation datum_tutorial_reflection)
add_datum_example(10_scan_resume 10_scan_resume datum_tutorial_reflection)
add_datum_example(10_scan_resume_variation 10_scan_resume_variation datum_tutorial_reflection)
add_datum_example(11_discreet_log 11_discreet_log datum_tutorial_reflection)
add_datum_example(11_discreet_log_variation 11_discreet_log_variation datum_tutorial_reflection)

option(DATUM_TUTORIAL_BUILD_CODECS "Build examples using Datum's optional codec adapter" OFF)
option(DATUM_TUTORIAL_BUILD_RGW "Build the native RGW examples" OFF)
if(DATUM_TUTORIAL_BUILD_CODECS OR DATUM_TUTORIAL_BUILD_RGW)
  include(FetchContent)
  FetchContent_Declare(zpp_bits
    GIT_REPOSITORY https://github.com/eyalz800/zpp_bits.git
    GIT_TAG ef0d73ec4b2371040b5254b89521538d796ba0a2) # v4.5.1
  FetchContent_MakeAvailable(zpp_bits)

  # zpp_bits provides a header directory, without an upstream CMake target.
  add_library(datum_tutorial_zpp INTERFACE)
  target_link_libraries(datum_tutorial_zpp INTERFACE datum_tutorial_reflection)
  target_include_directories(datum_tutorial_zpp SYSTEM INTERFACE "${zpp_bits_SOURCE_DIR}")
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
