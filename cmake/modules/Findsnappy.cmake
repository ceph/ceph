# - Find Snappy
# Find the snappy compression library and includes
#
# SNAPPY_INCLUDE_DIR - where to find snappy.h, etc.
# SNAPPY_LIBRARIES - List of libraries when using snappy.
# SNAPPY_FOUND - True if snappy found.

find_package(PkgConfig QUIET REQUIRED)
pkg_search_module(PC_snappy
  snappy QUIET)

find_path(SNAPPY_INCLUDE_DIR
  NAMES snappy.h
  HINTS
    ${PC_snappy_INCLUDE_DIRS}
    ${SNAPPY_ROOT_DIR}/include)

find_library(SNAPPY_LIBRARIES
  NAMES snappy
  HINTS
    ${PC_snappy_LIBRARY_DIRS}
    ${SNAPPY_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(snappy
  DEFAULT_MSG SNAPPY_LIBRARIES SNAPPY_INCLUDE_DIR)

mark_as_advanced(
  SNAPPY_LIBRARIES
  SNAPPY_INCLUDE_DIR)

if(snappy_FOUND AND NOT (TARGET snappy::snappy))
  add_library(snappy::snappy UNKNOWN IMPORTED)
  set_target_properties(snappy::snappy PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${SNAPPY_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION "${SNAPPY_LIBRARIES}")
endif()
