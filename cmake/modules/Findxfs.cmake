# Try to find xfs
# Once done, this will define
#
# XFS_FOUND - system has Profiler
# XFS_INCLUDE_DIR - the Profiler include directories
# XFS_LIBRARIES - link these to use Profiler

if(XFS_INCLUDE_DIR AND XFS_LIBRARIES)
	set(XFS_FIND_QUIETLY TRUE)
endif(XFS_INCLUDE_DIR AND XFS_LIBRARIES)

INCLUDE(CheckCXXSymbolExists)

# include dir

find_path(XFS_INCLUDE_DIR xfs.h NO_DEFAULT_PATH PATHS
  /usr/include
  /usr/include/xfs
  /opt/local/include
  /usr/local/include
)


# finally the library itself
find_library(LIBXFS NAMES handle)
set(XFS_LIBRARIES ${LIBXFS})

# handle the QUIETLY and REQUIRED arguments and set XFS_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(xfs DEFAULT_MSG XFS_LIBRARIES XFS_INCLUDE_DIR)

mark_as_advanced(XFS_LIBRARIES XFS_INCLUDE_DIR)
