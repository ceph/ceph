include_directories(ssched) # ssched code
include_directories(../../src) # dmclock code
include_directories(../../support/src)
include_directories(${BOOST_INCLUDE_DIR})

set(local_flags "-Wall -pthread ${CMAKE_CXX_SIM_FLAGS}")

set(ssched_sim_srcs test_ssched.cc test_ssched_main.cc)
set(dmc_sim_srcs test_dmclock.cc test_dmclock_main.cc)
set(config_srcs config.cc str_list.cc ConfUtils.cc)

set_source_files_properties(${ssched_sim_srcs} ${dmc_sim_srcs} ${dmc_srcs} ${config_srcs}
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
  SOURCE ${ssched_sim_srcs} ${dmc_sim_srcs} ${config_srcs}
  APPEND_STRING
  PROPERTY COMPILE_FLAGS "${warnings_off}"
  )

add_executable(ssched_sim EXCLUDE_FROM_ALL ${ssched_sim_srcs})
add_executable(dmc_sim EXCLUDE_FROM_ALL ${dmc_sim_srcs} ${config_srcs})

set_target_properties(ssched_sim dmc_sim
  PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ..)

add_dependencies(dmc_sim dmclock)

target_link_libraries(ssched_sim LINK_PRIVATE pthread)
target_link_libraries(dmc_sim LINK_PRIVATE pthread $<TARGET_FILE:dmclock>)

add_custom_target(dmclock-sims DEPENDS ssched_sim dmc_sim)
