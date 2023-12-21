# - Find uring
#
# URING_INCLUDE_DIR - Where to find liburing.h
# URING_LIBRARIES - List of libraries when using uring.
# uring_FOUND - True if uring found.

find_path(URING_INCLUDE_DIR liburing.h)
find_library(URING_LIBRARIES uring)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uring DEFAULT_MSG URING_LIBRARIES URING_INCLUDE_DIR)

if(uring_FOUND AND NOT TARGET uring::uring)
  add_library(uring::uring UNKNOWN IMPORTED)
  set_target_properties(uring::uring PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${URING_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${URING_LIBRARIES}")
endif()

mark_as_advanced(URING_INCLUDE_DIR URING_LIBRARIES)
