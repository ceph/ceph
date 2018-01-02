include_directories(SYSTEM ${GTEST_INCLUDE_DIRS})
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
include_directories(../support/src)

set(CMAKE_CXX_FLAGS
  "${CMAKE_CXX_FLAGS} -std=c++11 -Wno-write-strings -Wall -pthread")

set(dmc_srcs dmclock_util.cc ../support/src/run_every.cc)

add_library(dmclock STATIC ${dmc_srcs})
