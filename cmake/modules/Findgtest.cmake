# - Find gtest
#
#  GTEST_INCLUDE_DIRS   - where to find mcas/mcas.h, etc.
#  GTEST_LIBRARIES      - List of libraries when using mcas.
#  GTEST                - True if mcas found.

message(STATUS $ENV{HOME}/develop/usr/local/include)

find_path(GTEST_INCLUDE_DIR NAMES gtest/gtest.h gmock/gmock.h
    PATHS /usr/local/include $ENV{HOME}/develop/usr/local/include)

find_library(GTEST_LIBRARY
  NAMES gtest
  PATHS /usr/local/lib $ENV{HOME}/develop/usr/local/lib)

find_library(GTEST_MAIN_LIBRARY
  NAMES gtest_main
  PATHS /usr/local/lib $ENV{HOME}/develop/usr/local/lib)

find_library(GMOCK_LIBRARY
  NAMES gmock
  PATHS /usr/local/lib $ENV{HOME}/develop/usr/local/lib)

find_library(GMOCK_MAIN_LIBRARY
  NAMES gmock_main
  PATHS /usr/local/lib $ENV{HOME}/develop/usr/local/lib)


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(gtest
  REQUIRED_VARS GTEST_LIBRARY GTEST_MAIN_LIBRARY GTEST_INCLUDE_DIR)

if(gtest_FOUND)
  set(GTEST_FOUND 1)
endif()
if(GTEST_FOUND)
#  set(GTEST_LIBRARIES ${GTEST_LIBRARY})
#  set(GTEST_INCLUDE_DIRS ${GTEST_INCLUDE_DIR})
endif()
