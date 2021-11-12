# - Find libcryptsetup
# Sets the following:
#
# LIBCRYPTSETUP_INCLUDE_DIR
# LIBCRYPTSETUP_LIBRARIES
# LIBCRYPTSETUP_VERSION
# LIBCRYPTSETUP_FOUND

find_package(PkgConfig QUIET REQUIRED)
pkg_search_module(PC_libcryptsetup libcryptsetup)

find_path(LIBCRYPTSETUP_INCLUDE_DIR
        NAMES libcryptsetup.h
        PATHS ${PC_libcryptsetup_INCLUDE_DIRS})

find_library(LIBCRYPTSETUP_LIBRARIES
        NAMES libcryptsetup.so
        PATHS ${PC_libcryptsetup_LIBRARY_DIRS})

set(LIBCRYPTSETUP_VERSION ${PC_libcryptsetup_VERSION})

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(libcryptsetup
        REQUIRED_VARS
        LIBCRYPTSETUP_INCLUDE_DIR
        LIBCRYPTSETUP_LIBRARIES
        VERSION_VAR LIBCRYPTSETUP_VERSION)

mark_as_advanced(
  LIBCRYPTSETUP_LIBRARIES
  LIBCRYPTSETUP_INCLUDE_DIR
  LIBCRYPTSETUP_VERSION)
