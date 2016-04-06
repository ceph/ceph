cmake_minimum_required(VERSION 2.8)


set(CMAKE_CXX_FLAGS "-std=gnu++11")
include_directories(src)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

if(CMAKE_BUILD_TYPE MATCHES Release)
  set(my_compile_flags "-O3 -Wno-write-strings")
else()
  set(my_compile_flags "-g -O0 -Wno-write-strings")
endif()

find_package(gtest REQUIRED)
if(GTEST_FOUND)
  set(CMAKE_CTEST_COMMAND ctest -V)
  add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})
  enable_testing()
  include_directories(${GTEST_INCLUDES})
  add_subdirectory(test)
else()
  error("Could not find gtest; cannot continue.")
endif(GTEST_FOUND)

find_package(boost REQUIRED)
if(NOT BOOST_FOUND)
  error("Could not find boost; cannot continue.")
endif(NOT BOOST_FOUND)

add_subdirectory(src)
