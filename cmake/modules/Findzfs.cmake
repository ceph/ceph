# find libzfs or libzfslinux
# Once done, this will define
#
# ZFS_FOUND - system has libzfs
# ZFS_INCLUDE_DIR - the libzfs include directories
# ZFS_LIBRARIES - link these to use libzfs

find_package(PkgConfig)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(ZFS QUIET libzfs)
else()
  find_path(ZFS_INCLUDE_DIR libzfs.h
    HINTS
      ENV ZFS_DIR
    PATH_SUFFIXES libzfs)

  find_library(ZFS_LIBRARIES
    NAMES zfs
    HINTS
      ENV ZFS_DIR)
  set(XFS_LIBRARIES ${LIBXFS})
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(zfs DEFAULT_MSG
  ZFS_INCLUDE_DIRS ZFS_LIBRARIES)

mark_as_advanced(ZFS_INCLUDE_DIRS XFS_LIBRARIES)
