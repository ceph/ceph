# SPDX-License-Identifier: LGPL-2.1-or-later

cmake_minimum_required(VERSION 3.22.1)

foreach(required CEPH_SOURCE_DIR TEST_ROOT TEST_GENERATOR)
  if(NOT DEFINED ${required} OR "${${required}}" STREQUAL "")
    message(FATAL_ERROR "${required} is required")
  endif()
endforeach()

function(run)
  execute_process(COMMAND ${ARGV}
    RESULT_VARIABLE status
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error)
  if(NOT status STREQUAL "0")
    message(FATAL_ERROR "${ARGV} failed (${status}):\n${output}\n${error}")
  endif()
endfunction()

string(RANDOM LENGTH 12 ALPHABET 0123456789abcdef nonce)
set(root "${TEST_ROOT}/fusestore-${nonce}")
file(MAKE_DIRECTORY "${root}/source/os/memstore")

# Exercise the real target definitions with small translation units so this
# checks generated-header ordering without requiring Ceph's dependencies.
configure_file("${CEPH_SOURCE_DIR}/src/os/CMakeLists.txt"
  "${root}/source/os/CMakeLists.txt" COPYONLY)
configure_file("${CEPH_SOURCE_DIR}/src/os/memstore/CMakeLists.txt"
  "${root}/source/os/memstore/CMakeLists.txt" COPYONLY)
foreach(source ObjectStore Transaction FuseStore)
  file(WRITE "${root}/source/os/${source}.cc"
    "#include \"global_legacy_options.h\"\nint ${source}_fixture = 0;\n")
endforeach()
file(WRITE "${root}/source/os/memstore/MemStore.cc"
  "int memstore_fixture = 0;\n")
file(WRITE "${root}/source/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.22.1)
project(fusestore_dependencies LANGUAGES CXX)
set(WITH_FUSE ON)
set(header "${CMAKE_BINARY_DIR}/include/global_legacy_options.h")
add_custom_command(OUTPUT "${header}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/include"
  COMMAND "${CMAKE_COMMAND}" -E touch "${header}")
add_library(legacy-option-headers INTERFACE)
target_sources(legacy-option-headers PRIVATE "${header}")
include_directories("${CMAKE_BINARY_DIR}/include")
add_library(FUSE::FUSE INTERFACE IMPORTED)
add_subdirectory(os)
]=])

set(configure_args)
if(DEFINED TEST_MAKE_PROGRAM)
  list(APPEND configure_args "-DCMAKE_MAKE_PROGRAM=${TEST_MAKE_PROGRAM}")
endif()
if(DEFINED TEST_CXX_COMPILER)
  list(APPEND configure_args "-DCMAKE_CXX_COMPILER=${TEST_CXX_COMPILER}")
endif()
run("${CMAKE_COMMAND}" -S "${root}/source" -B "${root}/build"
  -G "${TEST_GENERATOR}" ${configure_args})

# Building fusestore alone must generate its headers. Building os first would
# hide the missing edge because os already depends on legacy-option-headers.
run("${CMAKE_COMMAND}" --build "${root}/build"
  --target fusestore --parallel 2 --config Debug)
file(REMOVE "${root}/build/include/global_legacy_options.h")
run("${CMAKE_COMMAND}" --build "${root}/build"
  --target fusestore --parallel 2 --config Debug)

file(REMOVE_RECURSE "${root}")
message(STATUS "FuseStore generated-header dependencies passed")
