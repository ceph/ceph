# - Find ZBD
#
# ZBD_INCLUDE - Where to find zbd.h
# ZBD_LIBRARIES - List of libraries when using zbd.
# ZBD_FOUND - True if zbd found.

find_path(ZBD_INCLUDE_DIR
  zbd.h
  HINTS $ENV{ZBD_ROOT}/libzbd
  PATH_SUFFIXES libzbd)

find_library(ZBD_LIBRARIES
  zbd
  HINTS $ENV{ZBD_ROOT}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(zbd DEFAULT_MSG ZBD_LIBRARIES ZBD_INCLUDE_DIR)

mark_as_advanced(ZBD_INCLUDE_DIR ZBD_LIBRARIES)
