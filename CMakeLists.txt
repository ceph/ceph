cmake_minimum_required(VERSION 2.8)

# set(CMAKE_MODULE_PATH
#   ${CMAKE_MODULE_PATH}
#  "${CMAKE_CURRENT_LIST_DIR}/cmake/modules/")

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
  include_directories(${GTEST_INCLUDE_DIRS})
endif(GTEST_FOUND)

add_subdirectory(src)
add_subdirectory(test)
