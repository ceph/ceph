cmake_minimum_required(VERSION 3.14.0)
project(dbstore-tests)

set(dbstore_tests_srcs
    dbstore_tests.cc)

add_executable(unittest_dbstore_tests ${dbstore_tests_srcs})
target_link_libraries(unittest_dbstore_tests
    dbstore
    gtest)
add_ceph_unittest(unittest_dbstore_tests)
if(COMPILER_SUPPORTS_VLA_ERROR)
  target_compile_options(unittest_dbstore_tests PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-Werror=vla>)
endif()

add_executable(unittest_dbstore_mgr_tests dbstore_mgr_tests.cc)
target_link_libraries(unittest_dbstore_mgr_tests
    dbstore
    gtest_main)
add_ceph_unittest(unittest_dbstore_mgr_tests)
if(COMPILER_SUPPORTS_VLA_ERROR)
  target_compile_options(unittest_dbstore_mgr_tests PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-Werror=vla>)
endif()
