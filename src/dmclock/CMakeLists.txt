cmake_minimum_required(VERSION 2.8.11)

set(CMAKE_CXX_FLAGS "-std=c++11 -Wno-write-strings ${CMAKE_CXX_FLAGS}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

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

if (NOT(TARGET gtest AND TARGET gtest_main))
  find_package(gtest REQUIRED)
  include_directories(${GTEST_INCLUDE_DIRS})
endif()

find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

add_subdirectory(src)
add_subdirectory(sim)
add_subdirectory(support)

enable_testing()
add_subdirectory(test)
