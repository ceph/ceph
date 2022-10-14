# - Find libcap
# Sets the following:
#
# LIBCAP_INCLUDE_DIR
# LIBCAP_LIBRARIES
# LIBCAP_VERSION
# LIBCAP_FOUND

find_package(PkgConfig QUIET REQUIRED)
pkg_search_module(PC_libcap libcap)

find_path(LIBCAP_INCLUDE_DIR
        NAMES sys/capability.h
        PATHS ${PC_libcap_INCLUDE_DIRS})

find_library(LIBCAP_LIBRARIES
        NAMES libcap.so
        PATHS ${PC_libcap_LIBRARY_DIRS})

set(LIBCAP_VERSION ${PC_libcap_VERSION})

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(libcap
        REQUIRED_VARS
        LIBCAP_INCLUDE_DIR
        LIBCAP_LIBRARIES
        VERSION_VAR LIBCAP_VERSION)

mark_as_advanced(
  LIBCAP_LIBRARIES
  LIBCAP_INCLUDE_DIR
  LIBCAP_VERSION)
