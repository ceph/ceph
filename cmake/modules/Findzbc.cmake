# - Find ZBC
#
# ZBC_INCLUDE - Where to find zbc.h
# ZBC_LIBRARIES - List of libraries when using zbc.
# ZBC_FOUND - True if zbc found.

find_path(ZBC_INCLUDE_DIR
  zbc.h
  HINTS $ENV{ZBC_ROOT}/libzbc
  PATH_SUFFIXES libzbc)

find_library(ZBC_LIBRARIES
  zbc
  HINTS $ENV{ZBC_ROOT}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(zbc DEFAULT_MSG ZBC_LIBRARIES ZBC_INCLUDE_DIR)

mark_as_advanced(ZBC_INCLUDE_DIR ZBC_LIBRARIES)
