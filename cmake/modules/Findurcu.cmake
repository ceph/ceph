# - Find URCU
#
# URCU_INCLUDE_DIRS - Where to find urcu.h
# URCU_LIBRARIES - List of libraries when using URCU.
# URCU_FOUND - True if URCU found.

find_path(URCU_INCLUDE_DIR NAMES urcu/uatomic.h)

find_library(URCU_LIBRARY NAMES urcu)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(urcu DEFAULT_MSG URCU_LIBRARY URCU_INCLUDE_DIR)

IF(URCU_FOUND)
  SET(URCU_LIBRARIES ${URCU_LIBRARY})
  SET(URCU_INCLUDE_DIRS ${URCU_INCLUDE_DIR})
ENDIF(URCU_FOUND)

MARK_AS_ADVANCED(URCU_INCLUDE_DIR URCU_LIBRARY)
