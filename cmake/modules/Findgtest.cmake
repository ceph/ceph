# - Find gtest
#
#  GTEST_INCLUDE_DIRS   - where to find mcas/mcas.h, etc.
#  GTEST_LIBRARIES      - List of libraries when using mcas.
#  GTEST_FOUND          - True if mcas found.
#
#  GMOCK_INCLUDE_DIRS   - where to find mcas/mcas.h, etc.
#  GMOCK_LIBRARIES      - List of libraries when using mcas.
#  GMOCK_FOUND          - True if mcas found.


## GTEST

find_path(GTEST_INCLUDE_DIRS NAMES gtest/gtest.h
    PATHS /usr/include /usr/local/include)

find_library(GTEST_LIBRARY gtest
  PATHS /usr/local/lib /usr/lib64)

find_library(GTEST_MAIN_LIBRARY gtest_main
  PATHS /usr/local/lib /usr/lib64)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(gtest
  REQUIRED_VARS GTEST_LIBRARY GTEST_MAIN_LIBRARY GTEST_INCLUDE_DIRS)

if(gtest_FOUND)
  set(GTEST_FOUND 1)
endif()

## GMOCK

find_path(GMOCK_INCLUDE_DIRS NAMES gmock/gmock.h
    PATHS /usr/include /usr/local/include)

find_library(GMOCK_LIBRARY gmock
  PATHS /usr/local/lib /usr/lib64)

find_library(GMOCK_MAIN_LIBRARY gmock_main
  PATHS /usr/local/lib /usr/lib64)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(gmock
  REQUIRED_VARS GMOCK_LIBRARY GMOCK_MAIN_LIBRARY GMOCK_INCLUDE_DIRS)

if(gmock_FOUND)
  set(GMOCK_FOUND 1)
endif()
