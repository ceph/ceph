# Try to find Profiler
# Once done, this will define
#
# PROFILER_FOUND - system has Profiler
# PROFILER_INCLUDE_DIR - the Profiler include directories
# PROFILER_LIBRARIES - link these to use Profiler
 
if(PROFILER_INCLUDE_DIR AND PROFILER_LIBRARIES)
set(PROFILER_FIND_QUIETLY TRUE)
endif(PROFILER_INCLUDE_DIR AND PROFILER_LIBRARIES)
 
INCLUDE(CheckCXXSymbolExists)
 
# include dir
 
find_path(PROFILER_INCLUDE_DIR profiler.h NO_DEFAULT_PATH PATHS
  ${HT_DEPENDENCY_INCLUDE_DIR}
  /usr/include
  /usr/include/google 
  /opt/local/include
  /usr/local/include
)


# finally the library itself
find_library(LIBPROFILER NAMES profiler)
set(PROFILER_LIBRARIES ${LIBPROFILER})
 
# handle the QUIETLY and REQUIRED arguments and set PROFILER_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PROFILER DEFAULT_MSG PROFILER_LIBRARIES PROFILER_INCLUDE_DIR)
 
mark_as_advanced(PROFILER_LIBRARIES PROFILER_INCLUDE_DIR)
