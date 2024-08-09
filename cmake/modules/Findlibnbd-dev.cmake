# - Find libnbd-dev
# Sets the following:
#
# LIBNBD_INCLUDE_DIR
# LIBNBD_LIBRARIES
# LIBNBD_VERSION

find_package(PkgConfig QUIET REQUIRED)
pkg_search_module(PC_libnbd-dev libnbd-dev)

find_path(LIBNBD_INCLUDE_DIR
        NAMES libnbd.h
        PATHS ${PC_libnbd-dev_INCLUDE_DIRS})

find_library(LIBNBD_LIBRARIES
        NAMES libnbd.so
        PATHS ${PC_libnbd-dev_LIBRARY_DIRS})

set(LIBNBD_VERSION ${PC_libnbd-dev_VERSION})

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(libnbd-dev
        REQUIRED_VARS
        LIBNBD_INCLUDE_DIR
        LIBNBD_LIBRARIES
        VERSION_VAR LIBNBD_VERSION)

mark_as_advanced(
  LIBNBD_LIBRARIES
  LIBNBD_INCLUDE_DIR
  LIBNBD_VERSION)
