# - Try to find blkin
# Once done this will define
#  BLKIN_FOUND - System has blkin
#  BLKIN_INCLUDE_DIR - The blkin include directories
#  BLKIN_LIBRARIES - The libraries needed to use blkin

find_package(PkgConfig)
pkg_check_modules(PC_BLKIN QUIET libblkin)

find_path(BLKIN_INCLUDE_DIR ztracer.hpp
	HINTS ${PC_BLKIN_INCLUDEDIR} ${PC_BLKIN_INCLUDE_DIRS}
	PATH_SUFFIXES blkin)
find_library(BLKIN_LIBRARY NAMES blkin
	HINTS ${PC_BLKIN_LIBDIR} ${PC_BLKIN_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set BLKIN_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(blkin DEFAULT_MSG
	BLKIN_LIBRARY BLKIN_INCLUDE_DIR)

set(BLKIN_LIBRARIES ${BLKIN_LIBRARY} lttng-ust)
mark_as_advanced(BLKIN_INCLUDE_DIR BLKIN_LIBRARIES)
