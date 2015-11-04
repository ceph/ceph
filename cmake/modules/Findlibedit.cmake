# Try to find libedit
# Once done, this will define
#
# LIBEDIT_FOUND - system has Profiler
# LIBEDIT_INCLUDE_DIR - the Profiler include directories
# LIBEDIT_LIBRARIES - link these to use Profiler

if(LIBEDIT_INCLUDE_DIR AND LIBEDIT_LIBRARIES)
	set(LIBEDIT_FIND_QUIETLY TRUE)
endif(LIBEDIT_INCLUDE_DIR AND LIBEDIT_LIBRARIES)

INCLUDE(CheckCXXSymbolExists)

# include dir

find_path(LIBEDIT_INCLUDE_DIR histedit.h NO_DEFAULT_PATH PATHS
  /usr/include
  /opt/local/include
  /usr/local/include
)


# finally the library itself
find_library(LIBLIBEDIT NAMES edit)
set(LIBEDIT_LIBRARIES ${LIBLIBEDIT})

# handle the QUIETLY and REQUIRED arguments and set LIBEDIT_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libedit DEFAULT_MSG LIBEDIT_LIBRARIES LIBEDIT_INCLUDE_DIR)

mark_as_advanced(LIBEDIT_LIBRARIES LIBEDIT_INCLUDE_DIR)
