cmake_minimum_required(VERSION 2.8)

add_subdirectory(crimson)

include_directories(crimson/src)
include_directories(${BOOST_INCLUDE_DIR})

set(local_flags "-std=gnu++11 -Wall -pthread")

set(simple_sim_srcs test_simple.cc test_simple_main.cc)
set(dmc_sim_srcs test_dmclock.cc test_dmclock_main.cc)

set(dmc_srcs
  dmclock_server.cc dmclock_util.cc dmclock_recs.cc crimson/src/run_every.cc)

set_source_files_properties(${simple_sim_srcs} ${dmc_sim_srcs} ${dmc_srcs}
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
# set(test_srcs test_server.cc test_client.cc test.cc)
set_source_files_properties(${simple_sim_srcs} ${dmc_sim_src}
  PROPERTIES
  COMPILE_FLAGS "${warnings_off}"
  )

add_executable(simple_sim ${simple_sim_srcs})
add_executable(dmc_sim ${dmc_sim_srcs} ${dmc_srcs})

# target_link_libraries(sim PUBLIC pthread)
target_link_libraries(simple_sim PUBLIC pthread)
target_link_libraries(dmc_sim PUBLIC pthread)

# file(COPY sim DESTINATION .)

# add_custom_target(runsim
#   DEPENDS sim
#   COMMAND $<TARGET_FILE:sim>
#  COMMAND printf '\\a\\a\\a')
