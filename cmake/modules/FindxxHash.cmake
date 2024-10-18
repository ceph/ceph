# - Find xxHash
#
# XXHASH_INCLUDE_DIR - Where to find libxxHash.h
# XXHASH_LIBRARIES - List of libraries when using xxHash.
# xxHash_FOUND - True if xxHash found.
# XXHASH_VERSION_STRING
# XXHASH_VERSION_MAJOR
# XXHASH_VERSION_MINOR
# XXHASH_VERSION_RELEASE

find_package(PkgConfig QUIET)
pkg_search_module(PC_xxhash xxhash QUIET)

find_path(XXHASH_INCLUDE_DIR
  NAMES xxhash.h
  HINTS ${PC_xxhash_INCLUDE_DIRS})

find_library(XXHASH_LIBRARIES
  NAMES xxhash
  HINTS ${PC_xxhash_LIBRARY_DIRS})

# parse version defines from header:
#define XXH_VERSION_MAJOR    0
#define XXH_VERSION_MINOR    8
#define XXH_VERSION_RELEASE  2
if(XXHASH_INCLUDE_DIR)
  foreach(ver "MAJOR" "MINOR" "RELEASE")
    file(STRINGS "${XXHASH_INCLUDE_DIR}/xxhash.h" XX_VER_LINE
      REGEX "^#define[ \t]+XXH_VERSION_${ver}[ \t]+[^ \t]+$")
    string(REGEX REPLACE "^#define[ \t]+XXH_VERSION_${ver}[ \t]+([0-9]*)$"
      "\\1" XXHASH_VERSION_${ver} "${XX_VER_LINE}")
    unset(XX_VER_LINE)
  endforeach()
  set(XXHASH_VERSION_STRING "${XXHASH_VERSION_MAJOR}.${XXHASH_VERSION_MINOR}.${XXHASH_VERSION_RELEASE}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(xxHash
  FOUND_VAR xxHash_FOUND
  REQUIRED_VARS XXHASH_LIBRARIES XXHASH_INCLUDE_DIR
  VERSION_VAR XXHASH_VERSION_STRING)

if(xxHash_FOUND AND NOT TARGET xxHash::xxhash)
  add_library(xxHash::xxhash UNKNOWN IMPORTED)
  set_target_properties(xxHash::xxhash PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${XXHASH_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${XXHASH_LIBRARIES}")
endif()

mark_as_advanced(XXHASH_INCLUDE_DIR XXHASH_LIBRARIES XXHASH_VERSION_STRING
  XXHASH_VERSION_MAJOR XXHASH_VERSION_MINOR XXHASH_VERSION_RELEASE)
