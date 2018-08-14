cmake_minimum_required(VERSION 3.5.1)

project(dmclock CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/modules")

if (NOT(TARGET gtest AND TARGET gtest_main))
  if (NOT GTEST_FOUND)
    find_package(GTest QUIET)
    if (NOT GTEST_FOUND)
      include(BuildGTest)
    endif()
  endif()
endif()

if (NOT(BOOST_FOUND))
  find_package(Boost REQUIRED)
endif()

find_package(Threads)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(src)

# Determine if dmclock is built as a subproject (using add_subdirectory)
# or if it is the master project.
set(MASTER_PROJECT FALSE)
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  set(MASTER_PROJECT TRUE)
endif()

option(dmclock_TEST "Generate test targets" ${MASTER_PROJECT})
if(dmclock_TEST)
  enable_testing()
  add_subdirectory(test)
  add_subdirectory(support/test)
  add_subdirectory(sim)
endif()
