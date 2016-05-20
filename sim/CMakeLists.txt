cmake_minimum_required(VERSION 2.8)

include_directories(../src)
include_directories(../support/src)
include_directories(${BOOST_INCLUDE_DIR})

set(local_flags "-Wall -pthread")

set(ssched_sim_srcs test_ssched.cc test_ssched_main.cc)
set(dmc_sim_srcs test_dmclock.cc test_dmclock_main.cc)

set(dmc_srcs ../src/dmclock_util.cc ../support/src/run_every.cc)

set_source_files_properties(${ssched_sim_srcs} ${dmc_sim_srcs} ${dmc_srcs}
  PROPERTIES
  COMPILE_FLAGS "${local_flags}"
  )

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  set(warnings_off " -Wno-unused-variable -Wno-unused-function")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(warnings_off " -Wno-unused-but-set-variable -Wno-unused-function")
endif()

# append warning flags to certain source files
set_property(
  SOURCE ${ssched_sim_srcs} ${dmc_sim_src}
  APPEND_STRING
  PROPERTY COMPILE_FLAGS "${warnings_off}"
  )

add_executable(ssched_sim ${ssched_sim_srcs})
add_executable(dmc_sim ${dmc_sim_srcs} ${dmc_srcs})

# target_link_libraries(sim PUBLIC pthread)
target_link_libraries(ssched_sim PUBLIC pthread)
target_link_libraries(dmc_sim PUBLIC pthread)
