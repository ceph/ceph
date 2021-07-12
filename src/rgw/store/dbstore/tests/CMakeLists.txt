cmake_minimum_required(VERSION 3.14.0)
project(dbstore-tests)

set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} gtest)

set(dbstore_tests_srcs
    dbstore_tests.cc)

include_directories(${CMAKE_INCLUDE_DIR})

add_executable(dbstore-tests ${dbstore_tests_srcs})
target_link_libraries(dbstore-tests ${CMAKE_LINK_LIBRARIES})
