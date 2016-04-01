cmake_minimum_required(VERSION 2.8)

add_subdirectory(crimson)

include_directories(crimson/src)

set(local_flags "-std=gnu++11 -Wall -pthread")

set(core_srcs
  dmclock_server.cc dmclock_util.cc dmclock_recs.cc crimson/src/run_every.cc)
set(test_srcs test_server.cc test_client.cc test.cc)

set_source_files_properties(${core_srcs}
  PROPERTIES
  COMPILE_FLAGS "${my_compile_flags} ${local_flags}"
  )

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
   set(warnings_off "-Wno-unused-variable -Wno-unused-function")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
   set(warnings_off " -Wno-unused-but-set-variable -Wno-unused-function")
endif()

# we can relax warnings a bit for testing code since we may keep
# multiple options open with unused variables or functions
set_source_files_properties(${test_srcs}
  PROPERTIES
  COMPILE_FLAGS "${my_compile_flags} ${local_flags} ${warnings_off}"
  )

add_executable(sim ${core_srcs} ${test_srcs})

target_link_libraries(sim PUBLIC pthread)

# file(COPY sim DESTINATION .)

add_custom_target(runsim
  DEPENDS sim
  COMMAND $<TARGET_FILE:sim>
  COMMAND printf '\\a\\a\\a')
