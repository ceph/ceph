# - Find execinfo
# Find the execinfo headers and libraries.
#
#  EXECINFO_INCLUDE_DIRS - where to find execinfo.h, etc.
#  EXECINFO_LIBRARIES    - List of libraries when using execinfo.
#  EXECINFO_FOUND        - True if execinfo found.

# Look for the header file.
FIND_PATH(EXECINFO_INCLUDE_DIR NAMES execinfo.h)

# Look for the library.
FIND_LIBRARY(EXECINFO_LIBRARY NAMES execinfo)

# handle the QUIETLY and REQUIRED arguments and set EXECINFO_FOUND to TRUE if
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(execinfo DEFAULT_MSG EXECINFO_LIBRARY EXECINFO_INCLUDE_DIR)

# Copy the results to the output variables.
IF(EXECINFO_FOUND)
  SET(EXECINFO_LIBRARIES ${EXECINFO_LIBRARY})
  SET(EXECINFO_INCLUDE_DIRS ${EXECINFO_INCLUDE_DIR})
ELSE(EXECINFO_FOUND)
  SET(EXECINFO_LIBRARIES)
  SET(EXECINFO_INCLUDE_DIRS)
ENDIF(EXECINFO_FOUND)

MARK_AS_ADVANCED(EXECINFO_INCLUDE_DIR EXECINFO_LIBRARY)
