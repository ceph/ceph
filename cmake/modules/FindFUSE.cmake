# This module can find FUSE Library
#
# The following variables will be defined for your use:
# - FUSE_FOUND : was FUSE found?
# - FUSE_INCLUDE_DIRS : FUSE include directory
# - FUSE_LIBRARIES : FUSE library

find_path(
    FUSE_INCLUDE_DIRS
    NAMES fuse_common.h fuse_lowlevel.h fuse.h
    PATHS /usr/local/include/osxfuse /usr/local/include
    PATH_SUFFIXES fuse)

set(fuse_names fuse)
if(APPLE)
  list(APPEND fuse_names libosxfuse.dylib)
endif()

find_library(FUSE_LIBRARIES
  NAMES ${fuse_names}
  PATHS /usr/local/lib64 /usr/local/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(FUSE DEFAULT_MSG
  FUSE_INCLUDE_DIRS FUSE_LIBRARIES)

mark_as_advanced(
  FUSE_INCLUDE_DIRS FUSE_LIBRARIES)

if(FUSE_FOUND)
  if(NOT TARGET FUSE::FUSE)
    add_library(FUSE::FUSE UNKNOWN IMPORTED)
    set_target_properties(FUSE::FUSE PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${FUSE_INCLUDE_DIRS}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${FUSE_LIBRARIES}"
      VERSION "${FUSE_VERSION}")
  endif()
endif()
