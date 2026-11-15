cmake_minimum_required(VERSION 3.22.1)

foreach(required IN ITEMS GPERF GENERATOR_MODULE CXX_COMPILER WORK_DIR)
  if(NOT DEFINED ${required} OR "${${required}}" STREQUAL "")
    message(FATAL_ERROR "${required} is required")
  endif()
endforeach()

function(run expect_failure)
  execute_process(COMMAND ${ARGN}
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
  if(expect_failure)
    if(result STREQUAL "0")
      message(FATAL_ERROR "Invalid gperf input was accepted")
    endif()
    return()
  endif()
  if(NOT result STREQUAL "0")
    message(FATAL_ERROR "Command failed: ${ARGN}\n${output}${error}")
  endif()
endfunction()

function(remember path prefix)
  file(TIMESTAMP "${path}" timestamp "%s")
  file(SHA256 "${path}" checksum)
  set(${prefix}_timestamp "${timestamp}" PARENT_SCOPE)
  set(${prefix}_checksum "${checksum}" PARENT_SCOPE)
endfunction()

function(expect_unchanged path timestamp checksum)
  file(TIMESTAMP "${path}" actual_timestamp "%s")
  file(SHA256 "${path}" actual_checksum)
  if(NOT actual_timestamp STREQUAL timestamp OR NOT actual_checksum STREQUAL checksum)
    message(FATAL_ERROR "Unchanged output was rewritten: ${path}")
  endif()
endfunction()

find_program(ninja NAMES ninja ninja-build)
find_program(make NAMES gmake make)

foreach(generator IN ITEMS Ninja "Unix Makefiles")
  set(build_tool "${ninja}")
  if(generator STREQUAL "Unix Makefiles")
    set(build_tool "${make}")
  endif()
  if(NOT build_tool)
    message(STATUS "Skipping unavailable generator: ${generator}")
    continue()
  endif()

  string(MAKE_C_IDENTIFIER "${generator}" name)
  set(source "${WORK_DIR}/${name}/source with spaces")
  set(build "${WORK_DIR}/${name}/build with spaces")
  file(REMOVE_RECURSE "${WORK_DIR}/${name}")
  file(MAKE_DIRECTORY "${source}")
  configure_file("${GENERATOR_MODULE}" "${source}/GenerateGperf.cmake" COPYONLY)

  set(project [=[
cmake_minimum_required(VERSION 3.22.1)
project(gperf_regression LANGUAGES CXX)
include("${CMAKE_CURRENT_SOURCE_DIR}/GenerateGperf.cmake")
set(GPERF "@GPERF@")
gperf_generate("${CMAKE_CURRENT_SOURCE_DIR}/keywords.gperf" keywords.h)
add_executable(consumer consumer.cc)
target_include_directories(consumer PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
set_source_files_properties(consumer.cc PROPERTIES
  OBJECT_DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/keywords.h")
]=])
  string(CONFIGURE "${project}" project @ONLY)
  file(WRITE "${source}/CMakeLists.txt" "${project}")
  set(keywords [=[
%language=C++
%define class-name keyword_hash
%define lookup-function-name lookup
%struct-type
%{
#include <cstddef>
#include <cstring>
using std::size_t;
%}
struct Keyword { const char* name; int value; };
%%
read, 1
write, 2
]=])
  file(WRITE "${source}/keywords.gperf" "${keywords}")
  file(WRITE "${source}/consumer.cc" [=[
#include <cstdlib>
#include "keywords.h"
int main(int argc, char** argv) {
  const auto* keyword = keyword_hash::lookup("read", 4);
  return argc != 2 || !keyword || keyword->value != std::atoi(argv[1]);
}
]=])

  run(FALSE "${CMAKE_COMMAND}" -S "${source}" -B "${build}" -G "${generator}"
    "-DCMAKE_MAKE_PROGRAM=${build_tool}" "-DCMAKE_CXX_COMPILER=${CXX_COMPILER}")
  run(FALSE "${CMAKE_COMMAND}" --build "${build}" --parallel 1)
  set(executable "${build}/consumer")
  if(CMAKE_HOST_WIN32)
    string(APPEND executable ".exe")
  endif()
  run(FALSE "${executable}" 1)
  file(GLOB objects "${build}/CMakeFiles/consumer.dir/*.o"
    "${build}/CMakeFiles/consumer.dir/*.obj")
  list(LENGTH objects object_count)
  if(NOT object_count EQUAL 1)
    message(FATAL_ERROR "Expected one consumer object: ${objects}")
  endif()
  list(GET objects 0 object)
  set(header "${build}/keywords.h")
  remember("${header}" header)
  remember("${object}" object)

  # Unchanged generation must not invalidate Ninja consumers. Make retains its
  # timestamp fallback, but the following invocation must be a no-op for both.
  run(FALSE "${CMAKE_COMMAND}" -E sleep 1)
  run(FALSE "${CMAKE_COMMAND}" -E touch "${source}/keywords.gperf")
  run(FALSE "${CMAKE_COMMAND}" --build "${build}" --parallel 1)
  file(SHA256 "${header}" actual_checksum)
  if(NOT actual_checksum STREQUAL header_checksum)
    message(FATAL_ERROR "Unchanged input changed generated contents")
  endif()
  if(generator STREQUAL "Ninja")
    expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")
    expect_unchanged("${object}" "${object_timestamp}" "${object_checksum}")
  endif()
  remember("${header}" header)
  remember("${object}" object)
  run(FALSE "${CMAKE_COMMAND}" -E sleep 1)
  run(FALSE "${CMAKE_COMMAND}" --build "${build}" --parallel 1)
  expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")
  expect_unchanged("${object}" "${object_timestamp}" "${object_checksum}")

  string(REPLACE "read, 1" "read, 7" keywords "${keywords}")
  file(WRITE "${source}/keywords.gperf" "${keywords}")
  run(FALSE "${CMAKE_COMMAND}" --build "${build}" --parallel 1)
  run(FALSE "${executable}" 7)
  file(SHA256 "${header}" actual_checksum)
  if(actual_checksum STREQUAL header_checksum)
    message(FATAL_ERROR "Keyword change did not update generated contents")
  endif()
  remember("${header}" header)
  remember("${object}" object)

  run(FALSE "${CMAKE_COMMAND}" -E sleep 1)
  file(APPEND "${source}/GenerateGperf.cmake" "\n# Dependency change\n")
  run(FALSE "${CMAKE_COMMAND}" --build "${build}" --parallel 1)
  if(generator STREQUAL "Ninja")
    expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")
    expect_unchanged("${object}" "${object_timestamp}" "${object_checksum}")
  endif()

  file(REMOVE "${header}")
  run(FALSE "${CMAKE_COMMAND}" --build "${build}" --parallel 1)
  run(FALSE "${executable}" 7)
  remember("${header}" header)
  remember("${object}" object)
  file(WRITE "${source}/keywords.gperf" "%invalid-directive\n")
  run(TRUE "${CMAKE_COMMAND}" --build "${build}" --parallel 1)
  expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")
  expect_unchanged("${object}" "${object_timestamp}" "${object_checksum}")
endforeach()
