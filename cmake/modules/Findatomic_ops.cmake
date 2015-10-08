# - Find atomic_ops
# Find the native ATOMIC_OPS headers and libraries.
#
#  ATOMIC_OPS_INCLUDE_DIRS - where to find atomic_ops.h, etc.
#  ATOMIC_OPS_LIBRARIES    - List of libraries when using atomic_ops.
#  ATOMIC_OPS_FOUND        - True if atomic_ops found.

# Look for the header file.
FIND_PATH(ATOMIC_OPS_INCLUDE_DIR NAMES atomic_ops.h)

# Look for the library.
FIND_LIBRARY(ATOMIC_OPS_LIBRARY NAMES atomic_ops)

# handle the QUIETLY and REQUIRED arguments and set ATOMIC_OPS_FOUND to TRUE if
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(atomic_ops DEFAULT_MSG ATOMIC_OPS_LIBRARY ATOMIC_OPS_INCLUDE_DIR)

# Copy the results to the output variables.
IF(ATOMIC_OPS_FOUND)
  SET(ATOMIC_OPS_LIBRARIES ${ATOMIC_OPS_LIBRARY})
  SET(ATOMIC_OPS_INCLUDE_DIRS ${ATOMIC_OPS_INCLUDE_DIR})
ELSE(ATOMIC_OPS_FOUND)
  SET(ATOMIC_OPS_LIBRARIES)
  SET(ATOMIC_OPS_INCLUDE_DIRS)
ENDIF(ATOMIC_OPS_FOUND)

MARK_AS_ADVANCED(ATOMIC_OPS_INCLUDE_DIR ATOMIC_OPS_LIBRARY)
