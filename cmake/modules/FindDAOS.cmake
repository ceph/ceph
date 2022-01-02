# - Try to find DAOS
# Once done this will define
#  DAOS_FOUND - System has DAOS
#  DAOS_INCLUDE_DIRS - The DAOS include directories
#  DAOS_LIBRARIES - The libraries needed to use DAOS

# Uncomment when DAOS provides .pc files
#find_package(PkgConfig)
#pkg_check_modules(PC_DAOS daos)

find_path(DAOS_INCLUDE_DIR daos.h
  HINTS ${PC_DAOS_INCLUDEDIR} ${PC_DAOS_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include)

find_path(DAOS_FS_INCLUDE_DIR daos_fs.h
  HINTS ${PC_DAOS_INCLUDEDIR} ${PC_DAOS_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include)

find_path(DAOS_FS_INCLUDE_DIR daos_s3.h
  HINTS ${PC_DAOS_INCLUDEDIR} ${PC_DAOS_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include)

find_library(DAOS_LIBRARY NAMES daos
  HINTS ${PC_DAOS_LIBDIR} ${PC_DAOS_LIBRARY_DIRS}
  PATHS /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib)

find_library(DAOS_FS_LIBRARY NAMES dfs
  HINTS ${PC_DAOS_LIBDIR} ${PC_DAOS_LIBRARY_DIRS}
  PATHS /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib)

find_library(DAOS_FS_LIBRARY NAMES ds3
  HINTS ${PC_DAOS_LIBDIR} ${PC_DAOS_LIBRARY_DIRS}
  PATHS /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib)

find_library(DAOS_UNS_LIBRARY NAMES duns
  HINTS ${PC_DAOS_LIBDIR} ${PC_DAOS_LIBRARY_DIRS}
  PATHS /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib)

set(DAOS_INCLUDE_DIRS ${DAOS_INCLUDE_DIR} ${DAOS_FS_INCLUDE_DIR})
set(DAOS_LIBRARIES ${DAOS_LIBRARY} ${DAOS_FS_LIBRARY} ${DAOS_UNS_LIBRARY})

include(FindPackageHandleStandardArgs)
include_directories( ${PC_DAOS_INCLUDEDIR} )
link_directories( ${PC_DAOS_LIBDIR} )

# handle the QUIETLY and REQUIRED arguments and set DAOS_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(DAOS DEFAULT_MSG
                                  DAOS_INCLUDE_DIR DAOS_FS_INCLUDE_DIR 
                                  DAOS_LIBRARY DAOS_FS_LIBRARY DAOS_UNS_LIBRARY)

mark_as_advanced(DAOS_INCLUDE_DIR DAOS_FS_INCLUDE_DIR 
                 DAOS_LIBRARY DAOS_FS_LIBRARY DAOS_UNS_LIBRARY)
