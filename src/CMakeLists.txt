include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
include_directories(../support/src)

set(CMAKE_CXX_FLAGS
  "${CMAKE_CXX_FLAGS} -std=c++11 -Wno-write-strings -Wall -pthread")

if(DO_NOT_DELAY_TAG_CALC)
  add_definitions(-DDO_NOT_DELAY_TAG_CALC)
endif()

set(dmc_srcs dmclock_util.cc ../support/src/run_every.cc)

add_library(dmclock STATIC ${dmc_srcs})
