# This module can find FUSE Library
#
# The following variables will be defined for your use:
# - FUSE_FOUND : was FUSE found?
# - FUSE_INCLUDE_DIRS : FUSE include directory
# - FUSE_LIBRARIES : FUSE library
# - FUSE_VERSION : the version of the FUSE library found

if(PACKAGE_FIND_VERSION AND PACKAGE_FIND_VERSION VERSION_LESS "3.0")
  set(fuse_names fuse)
  set(fuse_suffixes fuse)
else()
  set(fuse_names fuse3 fuse)
  set(fuse_suffixes fuse3 fuse)
endif()

if(APPLE)
  list(APPEND fuse_names libosxfuse.dylib)
  list(APPEND fuse_suffixes osxfuse)
endif()

find_package(PkgConfig QUIET REQUIRED)
pkg_search_module(FUSE IMPORTED_TARGET GLOBAL QUIET ${fuse_names})

if(FUSE_FOUND)
  add_library(FUSE::FUSE INTERFACE IMPORTED)
  target_link_libraries(FUSE::FUSE INTERFACE PkgConfig::FUSE)
  set(FUSE_LIBRARY ${FUSE_LIBRARIES})
  set(FUSE_INCLUDE_DIR ${FUSE_INCLUDE_DIRS})
  string(REGEX REPLACE "([0-9]+)\.([0-9]+)\.([0-9]+)"
    "\\1" FUSE_MAJOR_VERSION "${FUSE_VERSION}")
  string(REGEX REPLACE "([0-9]+)\.([0-9]+)\.([0-9]+)"
    "\\2" FUSE_MINOR_VERSION "${FUSE_VERSION}")
endif()

if(NOT FUSE_INCLUDE_DIR)
  find_path(
    FUSE_INCLUDE_DIR
    NAMES fuse_common.h fuse_lowlevel.h fuse.h
    PATH_SUFFIXES ${fuse_suffixes}
    NO_DEFAULT_PATH)
endif()

if(FUSE_INCLUDE_DIR AND NOT FUSE_VERSION)
  foreach(ver "MAJOR" "MINOR")
    file(STRINGS "${FUSE_INCLUDE_DIR}/fuse_common.h" fuse_ver_${ver}_line
      REGEX "^#define[\t ]+FUSE_${ver}_VERSION[\t ]+[0-9]+$")
    string(REGEX REPLACE ".*#define[\t ]+FUSE_${ver}_VERSION[\t ]+([0-9]+)$"
      "\\1" FUSE_${ver}_VERSION "${fuse_ver_${ver}_line}")
  endforeach()
  set(FUSE_VERSION
    "${FUSE_MAJOR_VERSION}.${FUSE_MINOR_VERSION}")
endif()

if(NOT FUSE_LIBRARY)
  find_library(FUSE_LIBRARY
    NAMES ${fuse_names}
    NO_DEFAULT_PATH)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(FUSE
  REQUIRED_VARS FUSE_LIBRARY FUSE_INCLUDE_DIR
  VERSION_VAR FUSE_VERSION)

mark_as_advanced(
  FUSE_INCLUDE_DIR
  FUSE_LIBRARY)

if(FUSE_FOUND)
  set(FUSE_LIBRARIES ${FUSE_LIBRARY})
  set(FUSE_INCLUDE_DIRS ${FUSE_INCLUDE_DIR})
  if(NOT TARGET FUSE::FUSE)
    add_library(FUSE::FUSE UNKNOWN IMPORTED)
    set_target_properties(FUSE::FUSE PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${FUSE_INCLUDE_DIRS}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${FUSE_LIBRARIES}"
      VERSION "${FUSE_VERSION}")
  endif()
endif()
