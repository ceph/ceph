set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "../cmake/modules/")

if (NOT(TARGET gtest AND TARGET gtest_main))
  find_package(gtest REQUIRED)
  include_directories(${GTEST_INCLUDE_DIRS})
endif()

find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

include_directories(../support/src)
include_directories(${BOOST_INCLUDE_DIR})

set(local_flags "-std=c++11 -Wno-write-strings -Wall -pthread")

if(DO_NOT_DELAY_TAG_CALC)
  set(local_flags "${local_flags} -DDO_NOT_DELAY_TAG_CALC")
endif()

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
