# Try to find libuuid
# Once done, this will define
#
# UUID_FOUND - system has Profiler
# UUID_INCLUDE_DIR - the Profiler include directories
# UUID_LIBRARIES - link these to use Profiler

if(UUID_INCLUDE_DIR AND UUID_LIBRARIES)
	set(UUID_FIND_QUIETLY TRUE)
endif(UUID_INCLUDE_DIR AND UUID_LIBRARIES)

INCLUDE(CheckCXXSymbolExists)

# include dir

find_path(UUID_INCLUDE_DIR uuid.h NO_DEFAULT_PATH PATHS
  /usr/include
  /usr/include/uuid
  /opt/local/include
  /usr/local/include
)


# finally the library itself
find_library(LIBUUID NAMES uuid)
set(UUID_LIBRARIES ${LIBUUID})

# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libuuid DEFAULT_MSG UUID_LIBRARIES UUID_INCLUDE_DIR)

mark_as_advanced(UUID_LIBRARIES UUID_INCLUDE_DIR)
