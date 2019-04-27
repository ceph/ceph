# CMake module to search for liboath headers
#
# If it's found it sets OATH_FOUND to TRUE
# and following variables are set:
# OATH_INCLUDE_DIRS
# OATH_LIBRARIES
find_path(OATH_INCLUDE_DIR
  liboath/oath.h
  PATHS
  /usr/include
  /usr/local/include)
find_library(OATH_LIBRARY NAMES oath liboath PATHS
  /usr/local/lib
  /usr/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(OATH DEFAULT_MSG OATH_LIBRARY OATH_INCLUDE_DIR)

mark_as_advanced(OATH_LIBRARY OATH_INCLUDE_DIR)

if(OATH_FOUND)
  set(OATH_INCLUDE_DIRS "${OATH_INCLUDE_DIR}")
  set(OATH_LIBRARIES "${OATH_LIBRARY}")
  if(NOT TARGET OATH::OATH)
    add_library(OATH::OATH UNKNOWN IMPORTED)
  endif()
  set_target_properties(OATH::OATH PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${OATH_INCLUDE_DIRS}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${OATH_LIBRARIES}")
endif()
