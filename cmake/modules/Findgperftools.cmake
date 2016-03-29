# Try to find Profiler
# Once done, this will define
#
# GPERFTOOLS_FOUND - system has Profiler
# GPERFTOOLS_INCLUDE_DIR - the Profiler include directories
# GPERFTOOLS_LIBRARIES - link these to use Profiler

if(GPERFTOOLS_INCLUDE_DIR AND GPERFTOOLS_LIBRARIES)
	set(GPERFTOOLS_FIND_QUIETLY TRUE)
endif(GPERFTOOLS_INCLUDE_DIR AND GPERFTOOLS_LIBRARIES)

INCLUDE(CheckCXXSymbolExists)

# include dir

find_path(GPERFTOOLS_INCLUDE_DIR profiler.h NO_DEFAULT_PATH PATHS
  /usr/include
  /usr/include/gperftools
  /usr/include/google
  /opt/local/include
  /usr/local/include
)


# finally the library itself
find_library(LIBPROFILER NAMES profiler)
CHECK_INCLUDE_FILES("google/profiler.h" HAVE_PROFILER_H)
set(GPERFTOOLS_LIBRARIES ${LIBPROFILER})

# handle the QUIETLY and REQUIRED arguments and set GPERFTOOLS_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gperftools DEFAULT_MSG GPERFTOOLS_LIBRARIES GPERFTOOLS_INCLUDE_DIR)

mark_as_advanced(GPERFTOOLS_LIBRARIES GPERFTOOLS_INCLUDE_DIR)
