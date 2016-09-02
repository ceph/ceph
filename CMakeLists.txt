cmake_minimum_required(VERSION 2.8.11)

set(CMAKE_CXX_FLAGS "-std=c++11 -Wno-write-strings ${CMAKE_CXX_FLAGS}")

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

if(DO_NOT_DELAY_TAG_CALC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDO_NOT_DELAY_TAG_CALC")
endif()

if(K_WAY_HEAP)
  if(K_WAY_HEAP LESS 2)
    message(FATAL_ERROR "K_WAY_HEAP value should be at least 2")
  else()
    set(CMAKE_CXX_SIM_FLAGS "-DK_WAY_HEAP=${K_WAY_HEAP}")
  endif()
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
