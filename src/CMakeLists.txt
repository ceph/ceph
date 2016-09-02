include_directories(../support/src)
include_directories(${BOOST_INCLUDE_DIR})

set(local_flags "-Wall -pthread")

set(dmc_srcs dmclock_util.cc ../support/src/run_every.cc)

set_source_files_properties(${dmc_srcs}
  PROPERTIES
  COMPILE_FLAGS "${local_flags}"
  )

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  set(warnings_off " -Wno-unused-variable -Wno-unused-function")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(warnings_off " -Wno-unused-but-set-variable -Wno-unused-function")
endif()

add_library(dmclock STATIC ${dmc_srcs})

# file(COPY sim DESTINATION .)

# add_custom_target(runsim
#   DEPENDS sim
#   COMMAND $<TARGET_FILE:sim>
#  COMMAND printf '\\a\\a\\a')
