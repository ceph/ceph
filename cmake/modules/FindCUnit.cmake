# Try to find CUnit
#
# Once done, this will define
#
# CUNIT_FOUND

find_path(CUNIT_INCLUDE_DIR NAMES CUnit/CUnit.h)

find_library(CUNIT_LIBRARY NAMES
    cunit
    libcunit
    cunitlib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CUnit
  DEFAULT_MSG CUNIT_LIBRARY CUNIT_INCLUDE_DIR)

if(CUNIT_FOUND)
  set(CUNIT_LIBRARIES ${CUNIT_LIBRARY})
  set(CUNIT_INCLUDE_DIRS ${CUNIT_INCLUDE_DIR})
endif()

mark_as_advanced(CUNIT_INCLUDE_DIR CUNIT_LIBRARY)
