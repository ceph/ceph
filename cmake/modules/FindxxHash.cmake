# - Find xxHash
#
# XXHASH_INCLUDE_DIR - Where to find libxxHash.h
# XXHASH_LIBRARIES - List of libraries when using xxHash.
# xxHash_FOUND - True if xxHash found.

find_package(PkgConfig QUIET)
pkg_search_module(PC_xxhash xxhash QUIET)

find_path(XXHASH_INCLUDE_DIR
  NAMES xxhash.h
  HINTS ${PC_xxhash_INCLUDE_DIRS})

find_library(XXHASH_LIBRARIES
  NAMES xxhash
  HINTS ${PC_xxhash_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(xxHash DEFAULT_MSG XXHASH_LIBRARIES XXHASH_INCLUDE_DIR)

if(xxHash_FOUND AND NOT TARGET xxHash::xxhash)
  add_library(xxHash::xxhash UNKNOWN IMPORTED)
  set_target_properties(xxHash::xxhash PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${XXHASH_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${XXHASH_LIBRARIES}")
endif()

mark_as_advanced(XXHASH_INCLUDE_DIR XXHASH_LIBRARIES)
