cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_FLAGS "-std=c++11 -fdiagnostics-color=always -Wno-write-strings")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

if(CMAKE_BUILD_TYPE MATCHES Release)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wno-write-strings")
elseif(CMAKE_BUILD_TYPE MATCHES Debug)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -Wno-write-strings")
else()
  message(STATUS "build type '${CMAKE_BUILD_TYPE}' is neither 'Release' nor 'Debug'; assuming Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -Wno-write-strings")
endif()

if(PROFILE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPROFILE")
endif()

add_subdirectory(src)
add_subdirectory(sim)

function(dmclock_config_testing)
  set(CMAKE_CTEST_COMMAND ctest -V)
  add_custom_target(dmclock-check COMMAND ${CMAKE_CTEST_COMMAND})
  enable_testing()
  include_directories(${GTEST_INCLUDES})
  add_subdirectory(test)
endfunction()

if (TARGET gtest AND TARGET gtest_main)
  dmclock_config_testing()
else()
  find_package(gtest REQUIRED)
  dmclock_config_testing()
endif()

find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

add_subdirectory(support)
