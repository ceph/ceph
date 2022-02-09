cmake_minimum_required(VERSION 3.14.0)
project(dbstore-tests)

set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} gtest)

set(dbstore_tests_srcs
    dbstore_tests.cc)

include_directories(${CMAKE_INCLUDE_DIR})

add_executable(unittest_dbstore_tests ${dbstore_tests_srcs})
target_link_libraries(unittest_dbstore_tests ${CMAKE_LINK_LIBRARIES})
add_ceph_unittest(unittest_dbstore_tests)
