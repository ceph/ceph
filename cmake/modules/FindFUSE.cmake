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

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_search_module(PKG_FUSE QUIET ${fuse_names})

  string(REGEX REPLACE "([0-9]+)\.([0-9]+)\.([0-9]+)"
    "\\1" FUSE_MAJOR_VERSION "${PKG_FUSE_VERSION}")
  string(REGEX REPLACE "([0-9]+)\.([0-9]+)\.([0-9]+)"
    "\\2" FUSE_MINOR_VERSION "${PKG_FUSE_VERSION}")

  find_path(
    FUSE_INCLUDE_DIR
    NAMES fuse_common.h fuse_lowlevel.h fuse.h
    HINTS ${PKG_FUSE_INCLUDE_DIRS}
    PATH_SUFFIXES ${fuse_suffixes}
    NO_DEFAULT_PATH)

  find_library(FUSE_LIBRARIES
    NAMES ${fuse_names}
    HINTS ${PKG_FUSE_LIBDIR}
    NO_DEFAULT_PATH)
else()
  find_path(
    FUSE_INCLUDE_DIR
    NAMES fuse_common.h fuse_lowlevel.h fuse.h
    PATH_SUFFIXES ${fuse_suffixes})

  find_library(FUSE_LIBRARIES
    NAMES ${fuse_names}
    PATHS /usr/local/lib64 /usr/local/lib)

  foreach(ver "MAJOR" "MINOR")
    file(STRINGS "${FUSE_INCLUDE_DIR}/fuse_common.h" fuse_ver_${ver}_line
      REGEX "^#define[\t ]+FUSE_${ver}_VERSION[\t ]+[0-9]+$")
    string(REGEX REPLACE ".*#define[\t ]+FUSE_${ver}_VERSION[\t ]+([0-9]+)$"
      "\\1" FUSE_${ver}_VERSION "${fuse_ver_${ver}_line}")
  endforeach()
endif()

set(FUSE_VERSION
  "${FUSE_MAJOR_VERSION}.${FUSE_MINOR_VERSION}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(FUSE
  REQUIRED_VARS FUSE_LIBRARIES FUSE_INCLUDE_DIR
  VERSION_VAR FUSE_VERSION)

mark_as_advanced(
  FUSE_INCLUDE_DIR)

if(FUSE_FOUND)
  set(FUSE_INCLUDE_DIRS ${FUSE_INCLUDE_DIR})
  if(NOT TARGET FUSE::FUSE)
    add_library(FUSE::FUSE UNKNOWN IMPORTED)
    set_target_properties(FUSE::FUSE PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${FUSE_INCLUDE_DIRS}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${FUSE_LIBRARIES}"
      VERSION "${FUSE_VERSION}")
    if(APPLE)
      # macFUSE installs both libfuse2 and libfuse3, and ships a
      # /usr/local/include/fuse.h shim whose only content routes <fuse.h> to
      # the libfuse2 headers.  Apple clang searches /usr/local/include ahead
      # of every -isystem directory, so as a system include the fuse3 headers
      # can never win that lookup and <fuse.h> silently lands on fuse2, whose
      # types then collide with the fuse3 ones included elsewhere.  Handing
      # the directory over as -I puts it first.  The property needs CMake
      # 3.23; below that this is ignored and such a build has to point
      # FUSE_INCLUDE_DIR at one version only.
      set_target_properties(FUSE::FUSE PROPERTIES IMPORTED_NO_SYSTEM TRUE)
    endif()
  endif()
endif()
