cmake_minimum_required(VERSION 2.8.11)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/modules")

if (NOT(TARGET gtest AND TARGET gtest_main))
  if(NOT(GTEST_FOUND))
    find_package(GTest REQUIRED)
  endif()
endif()

if (NOT(BOOST_FOUND))
  find_package(Boost REQUIRED)
endif()

# add_subdirectory(support/src)
add_subdirectory(src)
add_subdirectory(sim)

enable_testing()
add_subdirectory(test)
add_subdirectory(support/test)
add_test(NAME dmclock-tests
  COMMAND $<TARGET_FILE:dmclock-tests>)
add_test(NAME dmclock-data-struct-tests
  COMMAND $<TARGET_FILE:dmclock-data-struct-tests>)
