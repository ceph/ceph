# Try to find libuuid
# Once done, this will define
#
# UUID_FOUND - system has Profiler
# UUID_INCLUDE_DIR - the Profiler include directories
# UUID_LIBRARIES - link these to use Profiler

if(UUID_INCLUDE_DIR AND UUID_LIBRARIES)
  set(UUID_FIND_QUIETLY TRUE)
endif()

find_path(UUID_INCLUDE_DIR NAMES uuid/uuid.h)
find_library(UUID_LIBRARIES NAMES uuid)
set(UUID_LIBRARIES ${LIBUUID})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uuid
  DEFAULT_MSG UUID_LIBRARIES UUID_INCLUDE_DIR)

mark_as_advanced(UUID_LIBRARIES UUID_INCLUDE_DIR)
