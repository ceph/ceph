# Find the native JeMalloc includes and library
# This module defines
#  JEMALLOC_INCLUDE_DIRS, where to find jemalloc.h, Set when
#                        JEMALLOC_INCLUDE_DIR is found.
#  JEMALLOC_LIBRARIES, libraries to link against to use JeMalloc.
#  JeMalloc_FOUND, If false, do not try to use JeMalloc.
#

find_path(JEMALLOC_INCLUDE_DIR jemalloc/jemalloc.h)

find_library(JEMALLOC_LIBRARIES jemalloc)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JeMalloc
  FOUND_VAR JeMalloc_FOUND
  REQUIRED_VARS JEMALLOC_LIBRARIES JEMALLOC_INCLUDE_DIR)

mark_as_advanced(
  JEMALLOC_INCLUDE_DIR
  JEMALLOC_LIBRARIES)

if(JeMalloc_FOUND AND NOT (TARGET JeMalloc::JeMalloc))
    add_library(JeMalloc::JeMalloc UNKNOWN IMPORTED)
    set_target_properties(JeMalloc::JeMalloc PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${JEMALLOC_INCLUDE_DIR}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${JEMALLOC_LIBRARIES}")
endif()
